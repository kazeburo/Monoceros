package Starcross::Server;

use strict;
use warnings;
use base qw/Plack::Handler::Starlet/;
use IO::Select;
use IO::Socket;
use IO::FDPass;
use Parallel::Prefork;
use AnyEvent;
use AnyEvent::Util qw(fh_nonblocking guard);
use AnyEvent::Handle;
use AnyEvent::Socket;
use File::Temp;

use Carp ();
use Plack::Util;
use POSIX qw(EINTR EAGAIN EWOULDBLOCK :sys_wait_h);
use Socket qw(IPPROTO_TCP TCP_NODELAY);
use Fcntl qw(LOCK_EX LOCK_NB LOCK_UN);

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
    my @pipe_lstn = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "failed to create socketpair: $!";
    my @pipe_worker = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "failed to create socketpair: $!";
    $self->{pipe_lstn} = \@pipe_lstn;
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

sub ae_queued_fdsend {
    my $self = shift;
    my $socket = shift;
    my $fh = shift;

    my $queue_key = sprintf "fd_send_queue_%d", fileno $socket;
    my $worker_key = sprintf "fd_send_worker_%d", fileno $socket;
    $self->{$queue_key} ||= [];

    push @{$self->{$queue_key}}, $fh;
    $self->{$worker_key} ||= AE::io $socket, 1, sub {
        do {
            if ( ! IO::FDPass::send(fileno $socket, fileno ${$self->{$queue_key}->[0]} ) ) {
                return if $! == Errno::EAGAIN || $! == Errno::EWOULDBLOCK;
                undef $self->{$worker_key};
                die "unable to pass file handle: $!"; 
            }
            shift @{$self->{$queue_key}};
        } while @{$self->{$queue_key}};
        undef $self->{$worker_key};
    };

    1;
}

sub connection_manager {
    my ($self, $worker_pid) = @_;
    #local $SIG{PIPE} = 'IGNORE';

    $self->{pipe_lstn}->[READER]->close;
    $self->{pipe_worker}->[WRITER]->close;
    fh_nonblocking $self->{listen_sock}, 1;
    fh_nonblocking $self->{pipe_worker}->[READER], 1;
    fh_nonblocking $self->{pipe_lstn}->[WRITER], 1;

    my %state;
    my %master;
    my $term_received = 0;

    my $cv = AE::cv;
    my $sig;$sig = AE::signal 'TERM', sub {
        delete $master{server_io}; #stop new accept
        kill 'TERM', $worker_pid;
        $term_received++;
        while ( keys %state ) {
            #waiting
        }
        $cv->send;
    };
    my $sig2;$sig2 = AE::signal 'USR1', sub {
        kill 'USR1', $worker_pid;
    };

    my $reqs = 0;
    $master{server_io} = AE::io $self->{listen_sock}, 0, sub {
        while ( $self->{listen_sock} && (my $fh = $self->{listen_sock}->accept) ) {
            fh_nonblocking $fh, 1;
            setsockopt($fh, IPPROTO_TCP, TCP_NODELAY, 1)
                or die "setsockopt(TCP_NODELAY) failed:$!";
            $reqs++;
            my $w; $w = AE::io $fh, 0, sub {
                $self->ae_queued_fdsend($self->{pipe_lstn}->[WRITER],\$fh);
                undef $w;
                --$reqs;
            };
        }
    };

    my $pipe_io = AE::io $self->{pipe_worker}->[READER], 0, sub {
        my $fd = IO::FDPass::recv(fileno $self->{pipe_worker}->[READER]);
        return if $fd < 0;
        my $fh = IO::Handle->new_from_fd($fd,'r+')
            or die "unable to convert file descriptor to handle: $!";
        if ( $term_received ) {
            return;
        }
        $reqs++;
        my $w; $w = AE::io $fh, 0, sub {
            $self->ae_queued_fdsend($self->{pipe_lstn}->[WRITER],\$fh);
            undef $w;
            --$reqs;
        };
    };
    $cv->recv;
    sub { $pipe_io };
}

sub request_worker {
    my ($self,$app) = @_;

    $self->{listen_sock}->close;
    $self->{pipe_lstn}->[WRITER]->close;
    $self->{pipe_worker}->[READER]->close;
    
    my ($lock_fh, $lock_filename) = File::Temp::tempfile(EXLOCK=>0, UNLINK=>0);
    my ($lock_fh2, $lock_filename2) = File::Temp::tempfile(EXLOCK=>0, UNLINK=>0);

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
            my $select_pipe_read = IO::Select->new($self->{pipe_lstn}->[READER]);
            my $max_reqs_per_child = $self->_calc_reqs_per_child();
            my $proc_req_count = 0;
            $self->{can_exit} = 1;

            local $SIG{TERM} = sub {
                exit 0 if $self->{can_exit};
                $self->{term_received}++;
                exit 0 if  $self->{term_received} > 1;
                
            };
            local $SIG{PIPE} = 'IGNORE';
            
            while ( $proc_req_count < $max_reqs_per_child ) {
                my @can_read = $select_pipe_read->can_read(1);
                next unless @can_read;
                flock $lock_fh, LOCK_EX|LOCK_NB or next;
                my $fd = IO::FDPass::recv(fileno $self->{pipe_lstn}->[READER]);
                flock $lock_fh, LOCK_UN;
                die "$!" if $fd < 0;
                ++$proc_req_count;
                my $conn = IO::Socket::INET->new_from_fd($fd,'r+');
                my $env = {
                    SERVER_PORT => $self->{port},
                    SERVER_NAME => $self->{host},
                    SCRIPT_NAME => '',
                    REMOTE_ADDR => $conn->peerhost,
                    REMOTE_PORT => $conn->peerport,
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
                my $keepalive = $self->handle_connection($env, $conn, $app, 1, 0);
                if ( !$self->{term_received} && $keepalive ) {
                    IO::FDPass::send(fileno $self->{pipe_worker}->[WRITER], fileno $conn)
                            or die "unable to pass file handle: $!";
                }
                $conn->close;
                undef $conn;
            }
        });
    }
    $pm->wait_all_children;
    close($lock_fh);
    unlink($lock_filename);
    close($lock_fh2);
    unlink($lock_filename2);

}


sub write_all {
    my $self = shift;
    my $length = $self->SUPER::write_all(@_);
    warn $length if $length != 216 && $length != 261;
    $length;
}
1;
