package Starcross::Server;

use strict;
use warnings;
use base qw/Plack::Handler::Starlet/;
use IO::Select;
use IO::Socket;
use IO::FDPass;
use Parallel::Prefork;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Util qw(fh_nonblocking guard);
use File::Temp;
use Digest::MD5;

use Carp ();
use Plack::Util;
use POSIX qw(EINTR EAGAIN EWOULDBLOCK :sys_wait_h);
use Socket qw(IPPROTO_TCP TCP_NODELAY);
use Fcntl qw(:flock);
use Scalar::Util qw/refaddr/;

use constant WRITER => 0;
use constant READER => 1;

sub run {
    my ($self, $app) = @_;
    $self->setup_listener();
    $self->setup_sockpair();
    $self->run_workers($app);
}

sub setup_sockpair {
    my $self = shift;
    my @pipe_lstn = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, 0)
        or die "failed to create socketpair: $!";
    my @pipe_worker = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, 0)
        or die "failed to create socketpair: $!";
    my @pipe_info = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, 0)
        or die "failed to create socketpair: $!";
    $self->{pipe_lstn} = \@pipe_lstn;
    $self->{pipe_info} = \@pipe_info;
    $self->{pipe_worker} = \@pipe_worker; 
    1;
}

sub run_workers {
    my ($self,$app) = @_;
    
    my $pid = fork;  
    my $blocker;
    if ( $pid ) {
        #parent
        $blocker = $self->connection_manager($pid);
    }
    elsif ( defined $pid ) {
        $self->request_worker($app);
    }
    else {
        die "failed fork:$!";
    }

    while (1) { 
        my $kid = waitpid(-1, WNOHANG);
        last if $kid < 1;
    }
    undef $blocker;
}

sub queued_fdsend {
    my $self = shift;
    my $conn = shift;

    $self->{fdsend_queue} ||= [];
    push @{$self->{fdsend_queue}},  $conn;
    
    $self->{fdsend_worker} ||= AE::io $self->{pipe_lstn}->[WRITER], 1, sub {
        do {
            # send fh
            if ( ! IO::FDPass::send(fileno $self->{pipe_lstn}->[WRITER], fileno $self->{fdsend_queue}->[0]) ) {
                return if $! == Errno::EAGAIN || $! == Errno::EWOULDBLOCK;
                undef $self->{fdsend_worker};
                die "unable to pass file handle: $!"; 
            }
            shift @{$self->{fdsend_queue}};
        } while @{$self->{fdsend_queue}};
        undef $self->{fdsend_worker};
    };

    1;
}

sub connection_manager {
    my ($self, $worker_pid) = @_;
    local $SIG{PIPE} = 'IGNORE';

    fh_nonblocking $self->{listen_sock}, 1;
    $self->{pipe_lstn}->[READER]->close;
    $self->{pipe_info}->[WRITER]->close;    
    $self->{pipe_worker}->[WRITER]->close;    
    fh_nonblocking $self->{pipe_lstn}->[WRITER], 1;
    fh_nonblocking $self->{pipe_worker}->[READER], 1;
    fh_nonblocking $self->{pipe_info}->[READER], 1;
    
    my %master;
    my $term_received = 0;
    my $reqs = 0;

    my $cv = AE::cv;
    my $sig;$sig = AE::signal 'TERM', sub {
        delete $master{server_io}; #stop new accept
        kill 'TERM', $worker_pid;
        $term_received++;
        while ( $reqs ) {
            #waiting
        }
        $cv->send;
    };
    my $sig2;$sig2 = AE::signal 'USR1', sub {
        delete $master{server_io}; #stop new accept
        kill 'USR1', $worker_pid;
        $cv->send;
    };

    my %sockets;
    #my $at; $at = AE::timer 0, 1, sub {
    #        warn scalar keys %sockets;
    #};
    my $hd = new AnyEvent::Handle 
        fh => $self->{pipe_info}->[READER];
    $hd->on_read(sub {
        shift->push_read( line => sub {
            #warn "[clean]" . $_[1];
            delete $sockets{$_[1]};
        });
    });
    
    $master{server_io} = AE::io $self->{listen_sock}, 0, sub {
        my ($fh,$peer) = $self->{listen_sock}->accept;
        return unless $fh;
        #warn "[manager]" . Digest::MD5::md5_hex($peer);
        $sockets{Digest::MD5::md5_hex($peer)} = [$fh,time];
        fh_nonblocking $fh, 1
            or die "failed to set socket to nonblocking mode:$!";
        setsockopt($fh, IPPROTO_TCP, TCP_NODELAY, 1)
            or die "setsockopt(TCP_NODELAY) failed:$!";
        $reqs++;
        my $w; $w = AE::io $fh, 0, sub {
            $self->queued_fdsend($fh);
            --$reqs;
            undef $w;
        };
    };

    $master{pipe_io} = AE::io $self->{pipe_worker}->[READER], 0, sub {
        my $fd = IO::FDPass::recv(fileno $self->{pipe_worker}->[READER]);
        return if $fd < 0;
        my $conn = IO::Socket::INET->new_from_fd($fd,'r')
            or die "unable to convert file descriptor to handle: $!";
        if ( $term_received ) {
            return;
        }
        my $remote = $conn->peername;
        return unless $remote;
        $sockets{Digest::MD5::md5_hex($remote)} = [$conn,time];
        $reqs++;
        my $w; $w = AE::io $conn, 0, sub {
            $self->queued_fdsend($conn);
            --$reqs;
            undef $w;
        };
    };
    $cv->recv;
    \%master;
}

sub request_worker {
    my ($self,$app) = @_;

    delete $self->{lock_fh};
    $self->{listen_sock}->close;
    $self->{pipe_lstn}->[WRITER]->close;
    $self->{pipe_worker}->[READER]->close;
    $self->{pipe_info}->[READER]->close;

    my ($tmp_lock_fh, $lock_filename) = File::Temp::tempfile(UNLINK=>0);
    close($tmp_lock_fh);
    $self->{lock_filename} = $lock_filename;

    # use Parallel::Prefork
    my %pm_args = (
        max_workers => $self->{max_workers},
        trap_signals => {
            TERM => 'TERM',
            HUP  => 'TERM',
        },
    );
    if (defined $self->{spawn_interval}) {
        $pm_args{trap_signals}{USR1} = [ 'TERM', $self->{spawn_interval} ];
        $pm_args{spawn_interval} = $self->{spawn_interval};
    }
    if (defined $self->{err_respawn_interval}) {
        $pm_args{err_respawn_interval} = $self->{err_respawn_interval};
    }

    my $pm = Parallel::Prefork->new(\%pm_args);

    while ($pm->signal_received !~ /^(TERM|USR1)$/) {
        $pm->start(sub {
            my $select_pipe_read = IO::Select->new(
                $self->{pipe_lstn}->[READER],
            );
            my $max_reqs_per_child = $self->_calc_reqs_per_child();
            my $proc_req_count = 0;
            $self->{can_exit} = 1;
            #open(my $lock_fh, '>', $self->{lock_filename}) or die $!;
            
            local $SIG{TERM} = sub {
                exit 0 if $self->{can_exit};
                $self->{term_received}++;
                exit 0 if  $self->{term_received} > 1;
                
            };
            local $SIG{PIPE} = 'IGNORE';
            
            while ( $proc_req_count < $max_reqs_per_child ) {
                my @can_read = $select_pipe_read->can_read(1);
                if ( !@can_read ) {
                    next;
                }
                #flock($lock_fh, LOCK_EX | LOCK_NB) or next;
                my $fd = IO::FDPass::recv(fileno $self->{pipe_lstn}->[READER]);
                die "couldnot read pipe: $!" if $fd < 0;
                #flock($lock_fh, LOCK_UN);
                
                ++$proc_req_count;
                my $conn = IO::Socket::INET->new_from_fd($fd,'r+')
                    or die "unable to convert file descriptor to handle: $!";
                #warn "[child]" . Digest::MD5::md5_hex($conn->peername);
                
                my ($peerhost, $peerport) = ($conn->peerhost, $conn->peerport);
                my $is_keepalive = 1;
                
                my $env = {
                    SERVER_PORT => $self->{port},
                    SERVER_NAME => $self->{host},
                    SCRIPT_NAME => '',
                    REMOTE_ADDR => $peerhost,
                    REMOTE_PORT => $peerport,
                    'psgi.version' => [ 1, 1 ],
                    'psgi.errors'  => *STDERR,
                    'psgi.url_scheme' => 'http',
                    'psgi.run_once'     => Plack::Util::FALSE,
                    'psgi.multithread'  => Plack::Util::FALSE,
                    'psgi.multiprocess' => Plack::Util::TRUE,
                    'psgi.streaming'    => Plack::Util::TRUE,
                    'psgi.nonblocking'  => Plack::Util::FALSE,
                    'psgix.input.buffered' => Plack::Util::TRUE,
                    'psgix.io'          => $conn,
                };
                $self->{_is_deferred_accept} = 1; #ready to read
                my $keepalive = $self->handle_connection($env, $conn, $app, 1, $is_keepalive);
                
                if ( !$self->{term_received} && $keepalive ) {
                    IO::FDPass::send(fileno $self->{pipe_worker}->[WRITER], fileno $conn)
                            or die "unable to pass file handle: $!";
                }
                else {
                    syswrite $self->{pipe_info}->[WRITER], Digest::MD5::md5_hex($conn->peername) . "\n";
                }
            }
        });
    }
    $pm->wait_all_children;
    unlink($self->{lock_filename});

}


1;
