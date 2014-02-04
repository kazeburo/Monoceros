package Monoceros::Server;

use strict;
use warnings;
use base qw/Plack::Handler::Starlet/;
use IO::Socket;
use IO::FDPass;
use Parallel::Prefork;
use AnyEvent;
use AnyEvent::Util qw(fh_nonblocking portable_socketpair);
use Time::HiRes qw/time/;
use Plack::TempBuffer;
use Plack::Util;
use Plack::HTTPParser qw( parse_http_request );
use POSIX qw(EINTR EAGAIN EWOULDBLOCK ESPIPE ENOBUFS setuid setgid :sys_wait_h);
use POSIX::getpeername qw/_getpeername/;
use POSIX::Socket;
use Socket qw(IPPROTO_TCP TCP_NODELAY);
use File::Temp qw/tempfile/;
use Digest::MD5 qw/md5/;
use Carp;

use constant WRITER => 0;
use constant READER => 1;

use constant S_GD => 0;
use constant S_FD => 1;
use constant S_TIME => 2;
use constant S_REQS => 3;
use constant S_STATE => 4; # 0:idle 1:queue

use constant MAX_REQUEST_SIZE => 131072;
use constant CHUNKSIZE    => 64 * 1024;
use constant DEBUG        => $ENV{MONOCEROS_DEBUG} || 0;

my $null_io = do { open my $io, "<", \""; $io };
my $have_accept4 = eval {
    require Linux::Socket::Accept4;
    Linux::Socket::Accept4::SOCK_CLOEXEC()|Linux::Socket::Accept4::SOCK_NONBLOCK();
};

my $have_sendfile = eval {
    require Sys::Sendfile;
};

sub new {
    my $class = shift;
    my %args = @_;
    # setup before instantiation
    my $listen_sock;
    if (defined $ENV{SERVER_STARTER_PORT}) {
        my ($hostport, $fd) = %{Server::Starter::server_ports()};
        if ($hostport =~ /(.*):(\d+)/) {
            $args{host} = $1;
            $args{port} = $2;
        } else {
            $args{port} = $hostport;
        }
        $listen_sock = IO::Socket::INET->new(
            Proto => 'tcp',
        ) or die "failed to create socket:$!";
        $listen_sock->fdopen($fd, 'w')
            or die "failed to bind to listening socket:$!";
    }
    my $max_workers = 5;
    for (qw(max_workers workers)) {
        $max_workers = delete $args{$_}
            if defined $args{$_};
    }

    if ($args{user}) {
        if ($args{user} !~ m/^\d*$/s) {
            $args{user} = getpwnam($args{user});
        }
        setuid($args{user});
    }
    if ($args{group}) {
        if ($args{group} !~ m/^\d*$/s) {
            $args{group} = getgrnam($args{user});
        }
        setgid($args{group});
    }
    # will daemonize
    if ($args{daemonize}) {
        my $pid = fork();
        chdir '/';
        exit if $pid;
    }
    # rename process

    $0 = 'Monoceros Master';
    
    my $open_max = eval { POSIX::sysconf (POSIX::_SC_OPEN_MAX ()) - 1 } || 1023;
    
    # will write pid_file
    if ($args{pid}) {
        write_pid($args{pid});
    }

    my $self = bless {
        host                 => $args{host} || 0,
        port                 => $args{port} || 8080,
        max_workers          => $max_workers,
        timeout              => $args{timeout} || 300,
        disable_keepalive    => (exists $args{keepalive} && !$args{keepalive}) ? 1 : 0,
        keepalive_timeout    => $args{keepalive_timeout} || 10,
        max_keepalive_connection => $args{max_keepalive_connection} || int($open_max/2),
        max_readahead_reqs   => (
            defined $args{max_readahead_reqs}
                ? $args{max_readahead_reqs} : 100
        ),
        min_readahead_reqs   => (
            defined $args{min_readahead_reqs}
                ? $args{min_readahead_reqs} : undef,
        ),
        server_software      => $args{server_software} || $class,
        server_ready         => $args{server_ready} || sub {},
        min_reqs_per_child   => (
            defined $args{min_reqs_per_child}
                ? $args{min_reqs_per_child} : undef,
        ),
        max_reqs_per_child   => (
            $args{max_reqs_per_child} || $args{max_requests} || 1000,
        ),
        err_respawn_interval => (
            defined $args{err_respawn_interval}
                ? $args{err_respawn_interval} : undef,
        ),
        _using_defer_accept  => 1,
        listen_sock => ( defined $listen_sock ? $listen_sock : undef),
    }, $class;

    $self;
}

sub run {
    my ($self, $app) = @_;
    $self->setup_listener();
    $self->setup_sockpair();
    $self->run_workers($app);
}

sub setup_sockpair {
    my $self = shift;

    my %workers;
    for my $wid ( 1..$self->{max_workers} ) {
        my @pair = portable_socketpair()
            or die "failed to create socketpair: $!";
        $workers{$wid} = {
            running => 0,
            sock => \@pair
        };
    }
    $self->{workers} = \%workers;

    my @fdpass_sock = portable_socketpair()
            or die "failed to create socketpair: $!";
    $self->{fdpass_sock} = \@fdpass_sock;

    my ($fh, $filename) = tempfile('monoceros_stats_XXXXXX',UNLINK => 0, SUFFIX => '.dat', TMPDIR => 1);
    $self->{stats_fh} = $fh;
    $self->{stats_filename} = $filename;
    $self->update_stats();

    1;
}

sub run_workers {
    my ($self,$app) = @_;
    local $SIG{PIPE} = 'IGNORE';    
    my $pid = fork;
    if ( $pid ) {
        #parent
        $self->connection_manager($pid);
        delete $self->{stats_fh};
        unlink $self->{stats_filename};
    }
    elsif ( defined $pid ) {
        $0 = 'Monoceros Worker';
        $self->request_worker($app);
        exit;
    }
    else {
        die "failed fork:$!";
    }
}

sub queued_send {
    my $self = shift;
    my $sockid = shift;

    if ( ! exists $self->{sockets}{$sockid} ) {
        return;
    }
    $self->{sockets}{$sockid}[S_STATE] = 1;

    push @{$self->{fdsend_queue}},  $sockid;
    $self->{fdsend_worker} ||= AE::io $self->{fdpass_sock}[WRITER], 1, sub {
        while ( my $sockid = shift @{$self->{fdsend_queue}} ) {
            if ( ! exists $self->{sockets}{$sockid}  ) {
                next;
            }
            if ( _getpeername($self->{sockets}{$sockid}[S_FD], my $addr) < 0 ) {
                delete $self->{sockets}{$sockid};
                next;
            }
            my $ret = IO::FDPass::send(
                fileno $self->{fdpass_sock}[WRITER],
                $self->{sockets}{$sockid}[S_FD]
            );
            if ( !$ret  ) {
                if ( $! == EAGAIN || $! == EWOULDBLOCK || $! == EINTR ) {
                    unshift @{$self->{fdsend_queue}}, $sockid;
                    return;
                }
                die "unable to pass queue: $!"; 
                undef $self->{fdsend_worker};
            }
        } 
        undef $self->{fdsend_worker};
    };
    1;
}

my $prev_stats = '';
sub update_stats {
    my $self = shift;
    my $total = scalar keys %{$self->{sockets}};
    my $processing = scalar grep { !$self->{sockets}{$_}[S_STATE] == 1 } keys %{$self->{sockets}};
    my $idle = scalar grep { $self->{sockets}{$_}[S_STATE] == 0 } keys %{$self->{sockets}};

    my $stats = "total=$total&";
    $stats .= "waiting=$idle&";
    $stats .= "processing=$processing&";
    $stats .= "max_workers=".$self->{max_workers}."&";
    return if $stats eq $prev_stats && @_;
    $prev_stats = $stats;
    seek($self->{stats_fh},0,0);
    syswrite($self->{stats_fh}, $stats);
}

sub can_keepalive {
    my $self = shift;
    seek($self->{stats_fh},0,0);
    sysread($self->{stats_fh},my $buf, 1024);
    return 1 unless $buf;
    if ( $buf =~ m!total=(\d+)&! ){
        return if $1 >= $self->{max_keepalive_connection};
    }
    return 1;
}

sub connection_manager {
    my ($self, $worker_pid) = @_;

    $self->{workers}{$_}{sock}[WRITER]->close for 1..$self->{max_workers};
    $self->{fdpass_sock}[READER]->close;
    fh_nonblocking $self->{fdpass_sock}[WRITER], 1;
    fh_nonblocking $self->{listen_sock}, 1;

    my %manager;
    my %hash2fd;
    my %wait_read;
    my $term_received = 0;
    $self->{sockets} = {};
    $self->{fdsend_queue} = [];

    warn sprintf "Set max_keepalive_connection to %s", $self->{max_keepalive_connection} if DEBUG;

    my $cv = AE::cv;
    my $close_all = 0;
    my $sig2;$sig2 = AE::signal 'USR1', sub {
        my $t;$t = AE::timer 0, 1, sub {
            return unless $close_all;
            undef $t;
            kill 'TERM', $worker_pid;
            my $t2;$t2 = AE::timer 0, 1, sub {
                my $kid = waitpid($worker_pid, WNOHANG);
                return if $kid >= 0;
                undef $t2;
                $cv->send;
            };
        };
    };
    my $sig;$sig = AE::signal 'TERM', sub {
        $term_received++;
        kill 'USR1', $worker_pid; #stop accept
        my $t;$t = AE::timer 0, 1, sub {
            my $time = time;
            return if keys %{$self->{sockets}};
            undef $t;
            $close_all=1;
        };
    };

    $manager{disconnect_keepalive_timeout} = AE::timer 0, 1, sub {
        my $time = time;
        if ( DEBUG ) {
            my $total = scalar keys %{$self->{sockets}};
            my $processing = scalar grep { $self->{sockets}{$_}[S_STATE] == 1} keys %{$self->{sockets}};
            my $idle = scalar grep { $self->{sockets}{$_}[S_STATE] == 0} keys %{$self->{sockets}};
            warn "working: $processing | total: $total | idle: $idle";
        }
        for my $key ( keys %{$self->{sockets}} ) { #key = fd
            if ( $self->{sockets}{$key}[S_STATE] == 0 && $self->{sockets}{$key}[S_REQS] == 0 
                     && $time - $self->{sockets}{$key}[S_TIME] > $self->{timeout} ) { #idle && first req 
                delete $wait_read{$key};
                delete $self->{sockets}{$key};
                
            }
            elsif ( $self->{sockets}{$key}[S_STATE] == 0 && $self->{sockets}{$key}[S_REQS] > 0 &&
                     $time - $self->{sockets}{$key}[S_TIME] > $self->{keepalive_timeout} ) { #idle && keepalivew
                delete $wait_read{$key};
                delete $self->{sockets}{$key};
            }
        }
        $self->update_stats(1);
    };

    my %m_state;
    my %workers;
    for my $wid ( 1..$self->{max_workers} ) {
        
        my $sock = $self->{workers}{$wid}{sock}[READER];
        fh_nonblocking($sock,1);
        return unless $sock;

        $m_state{$wid} = {};
        my $state = $m_state{$wid};
        $state->{buf} = '';
        $state->{state} = 'cmd';
        $state->{sockid} = '';
        $state->{reqs} = 0;
        
        $workers{$wid} = AE::io fileno $sock, 0, sub {
            if ( $state->{state} eq 'cmd' ) {
                my $ret = _recv(fileno($sock), my $buf, 28 - length($state->{buf}), 0);
                if ( !defined $ret && ($! == EINTR || $! == EAGAIN || $! == EWOULDBLOCK) ) {
                    return;
                }
                if ( !defined $ret ) {
                    warn "failed to recv from sock: $!";
                    return;                                        
                }
                if ( defined $buf && length $buf == 0) {
                    return;                   
                }
                $state->{buf} .= $buf;
                return if length $state->{buf} < 28;
                my $msg = substr $state->{buf}, 0, 28, '';
                my $method = substr($msg, 0, 4,'');
                my $sockid = substr($msg, 0, 16, '');
                my $reqs = hex($msg);

                if ( $method eq 'push' ) {
                    $state->{state} = 'recv_fd';
                    $state->{sockid} = $sockid;
                    $state->{reqs} = $reqs;
                }
                elsif ( $method eq 'keep' ) {
                    if ( exists $self->{sockets}{$sockid} ) {
                        $self->{sockets}{$sockid}[S_TIME] = AE::now;
                        $self->{sockets}{$sockid}[S_REQS] += $reqs;
                        $self->{sockets}{$sockid}[S_STATE] = 0;
                        $wait_read{$sockid} = AE::io $self->{sockets}{$sockid}[S_FD], 0, sub {
                            delete $wait_read{$sockid};
                            $self->queued_send($sockid);
                        };
                    }
                }
                elsif ( $method eq 'clos' ) {
                    delete $self->{sockets}{$sockid};
                    $self->update_stats();
                }
            }

            if ( $state->{state} eq 'recv_fd' ) {
                my $fd = IO::FDPass::recv(fileno $sock);
                if ( $fd < 0 && ($! == EINTR || $! == EAGAIN || $! == EWOULDBLOCK) ) {
                    return;
                }
                $state->{state} = 'cmd';
                if ( $fd <= 0 ) {
                    warn sprintf 'Failed recv fd: %s (%d)', $!, $!;
                    return;
                }
                my $sockid = $state->{sockid};
                my $reqs = $state->{reqs};
                $self->{sockets}{$sockid} = [
                    AnyEvent::Util::guard { _close($fd) },
                    $fd,
                    AE::now,
                    $reqs,
                    0
                ]; #guard,fd,time,reqs,state
                $self->update_stats();
                $wait_read{$sockid} = AE::io $fd, 0, sub {
                    delete $wait_read{$sockid};
                    $self->queued_send($sockid); 
                };
            } # cmd
        } # AE::io
    } # for 1..max_workers
    $manager{workers} = \%workers;
    $cv->recv;
}

sub request_worker {
    my ($self,$app) = @_;

    delete $self->{stats_fh};
    fh_nonblocking($self->{listen_sock},1);
    $self->{fdpass_sock}[WRITER]->close;
    fh_nonblocking($self->{fdpass_sock}[READER],1);
    $self->{workers}{$_}{sock}[READER]->close for 1..$self->{max_workers};

    # use Parallel::Prefork
    my %pm_args = (
        max_workers => $self->{max_workers},
        trap_signals => {
            TERM => 'TERM',
            HUP  => 'TERM',
            USR1 => 'USR1',
        },
    );
    if (defined $self->{err_respawn_interval}) {
        $pm_args{err_respawn_interval} = $self->{err_respawn_interval};
    }

    my $next;
    $pm_args{on_child_reap} = sub {
        my ( $pm, $exit_pid, $status ) = @_;
        for my $wid (1..$self->{max_workers} ) {
            if ( $self->{workers}{$wid}{running} && $self->{workers}{$wid}{running} == $exit_pid ) {
                #warn sprintf "finished wid:%s pid:%s", $next, $exit_pid if DEBUG;
                $self->{workers}{$wid}{running} = 0;
                last;
            }
        }
    };
    $pm_args{before_fork} = sub {
        for my $wid (1..$self->{max_workers} ) {
            if ( ! $self->{workers}{$wid}{running} ) {
                $next = $wid;
                last;
            }
        }

    };
    $pm_args{after_fork} = sub {
        my ($pm, $pid) = @_;
        if ( defined $next ) {
            #warn sprintf "assign wid:%s to pid:%s", $next, $pid if DEBUG;
            $self->{workers}{$next}{running} = $pid;
        }
        else {
            warn "worker start but next is undefined";
        }
    };

    my $pm = Parallel::Prefork->new(\%pm_args);

    while ($pm->signal_received !~ /^(?:TERM|USR1)$/) {
        $pm->start(sub {
            die 'worker start but next is undefined' unless $next;
            for my $wid ( 1..$self->{max_workers} ) {
                next if $wid == $next;
                $self->{workers}{$wid}{sock}[WRITER]->close;
            }
            $self->{mgr_sock} = $self->{workers}{$next}{sock}[WRITER];
            fh_nonblocking($self->{mgr_sock},1);

            open($self->{stats_fh}, '<', $self->{stats_filename})
                or die "could not open stats file: $!";

            $self->{fhlist} = [$self->{listen_sock},$self->{fdpass_sock}[READER]];
            $self->{fhbits} = '';
            for ( @{$self->{fhlist}} ) {
                vec($self->{fhbits}, fileno $_, 1) = 1;
            }
            
            my $max_reqs_per_child = $self->_calc_minmax_per_child(
                $self->{max_reqs_per_child},
                $self->{min_reqs_per_child}
            );
            my $max_readahead_reqs = $self->_calc_minmax_per_child(
                $self->{max_readahead_reqs},
                $self->{min_readahead_reqs}
            );

            my $proc_req_count = 0;
            
            $self->{term_received} = 0;
            $self->{stop_accept} = 0;
            local $SIG{TERM} = sub {
                $self->{term_received}++;
                exit 0 if $self->{term_received} > 1;
            };
            local $SIG{USR1} = sub {
                $self->{fhlist} = [$self->{fdpass_sock}[READER]];
                $self->{fhbits} = '';
                vec($self->{fhbits}, fileno($self->{fdpass_sock}[READER]), 1) = 1;
                $self->{stop_accept}++;
            };

            local $SIG{PIPE} = 'IGNORE';

            my $next_conn;

            while ( $next_conn || $self->{stop_accept} || $proc_req_count < $max_reqs_per_child ) {
                last if ( $self->{term_received} 
                       && !$next_conn );
                                
                my $conn;
                if ( $next_conn && $next_conn->{buf} ) { #read ahead or pipeline
                    $conn = $next_conn;
                    $next_conn = undef;
                }
                else {
                    my @rfh = @{$self->{fhlist}};
                    my $rfd = $self->{fhbits};
                    if ( $next_conn ) {
                        push @rfh, $next_conn->{fh};
                        vec($rfd, fileno $next_conn->{fh}, 1) = 1;
                    }                    
                    my @can_read;
                    if ( select($rfd, undef, undef, 1) > 0 ) {
                        for ( my $i = 0; $i <= $#rfh; $i++ ) {
                            my $try_read_fd = fileno $rfh[$i];
                            if ( !defined $rfd || vec($rfd, $try_read_fd, 1) ) {
                                if ( $next_conn && fileno $next_conn->{fh} == $try_read_fd ) {
                                    $conn = $next_conn;
                                    last;
                                }
                                push @can_read, $self->{fhlist}[$i];
                            }
                        }
                    }
                    #accept or recv
                    if ( !$conn )  {
                        $conn = $self->accept_or_recv( @can_read );
                    }
                    # exists new conn && exists next_conn && next_conn is not ready => keep
                    if ( $conn && $next_conn && $conn != $next_conn ) {
                        $self->keep_it($next_conn);
                    }
                    # try to re-read next_conn
                    if ( !$conn && $next_conn ) {
                        @rfh = ();
                        next;
                    }
                    #clear next_conn
                    @rfh = ();
                    $next_conn = undef;
                }
                next unless $conn;
                
                ++$proc_req_count;
                my $env = {
                    SERVER_PORT => $self->{port},
                    SERVER_NAME => $self->{host},
                    SCRIPT_NAME => '',
                    REMOTE_ADDR => $conn->{peeraddr},
                    REMOTE_PORT => $conn->{peerport},
                    'psgi.version'      => [ 1, 1 ],
                    'psgi.errors'       => *STDERR,
                    'psgi.url_scheme'   => 'http',
                    'psgi.run_once'     => Plack::Util::FALSE,
                    'psgi.multithread'  => Plack::Util::FALSE,
                    'psgi.multiprocess' => Plack::Util::TRUE,
                    'psgi.streaming'    => Plack::Util::TRUE,
                    'psgi.nonblocking'  => Plack::Util::FALSE,
                    'psgix.input.buffered' => Plack::Util::TRUE,
                    'psgix.io'          => $conn->{fh},
                    'psgix.harakiri'    => 1,
                    'X_MONOCEROS_WORKER_STATS' => $self->{stats_filename},
                };
                $self->{_is_deferred_accept} = 1; #ready to read
                my $prebuf;
                if ( exists $conn->{buf} ) {
                    $prebuf = delete $conn->{buf};
                }
                else {
                    #pre-read
                    my $ret = sysread($conn->{fh}, $prebuf, MAX_REQUEST_SIZE);
                    if ( ! defined $ret && ($! == EAGAIN || $! == EWOULDBLOCK || $! == EINTR) ) {
                        $self->keep_it($conn);
                        next;
                    }
                    elsif ( defined $ret && $ret == 0) {
                        #closed?
                        $self->cmd_to_mgr('clos', $conn->{peername}, $conn->{reqs}) 
                            if !$conn->{direct};
                        next;
                    }
                }
                # stop keepalive if SIG{TERM} or SIG{USR1}. but go-on if pipline req
                my $may_keepalive = 1;
                $may_keepalive = 0 if ($self->{term_received} || $self->{stop_accept});
                $may_keepalive = 0 if $self->{disable_keepalive};
                my $is_keepalive = 1; # to use "keepalive_timeout" in handle_connection, 
                                      #  treat every connection as keepalive
                my ($keepalive,$pipelined_buf) = $self->handle_connection($env, $conn->{fh}, $app, 
                                                         $may_keepalive, $is_keepalive, $prebuf, 
                                                         $conn->{reqs});
                # harakiri
                if ($env->{'psgix.harakiri.commit'}) {
                    $proc_req_count = $max_reqs_per_child + 1;
                }

                $conn->{reqs}++;
                if ( !$keepalive ) {
                    #close
                    $self->cmd_to_mgr('clos', $conn->{peername}, $conn->{reqs})
                        if !$conn->{direct};
                    next;
                }

                # pipeline
                if ( defined $pipelined_buf && length $pipelined_buf ) {
                    $next_conn = $conn;
                    $next_conn->{buf} = $pipelined_buf;
                    next;
                }

                # read ahead
                if ( $conn->{reqs} < $max_readahead_reqs &&  $proc_req_count < $max_reqs_per_child ) {
                    $next_conn = $conn;
                    next;
                }

                # wait
                $self->keep_it($conn);
            }
        }); #start
    }
    local $SIG{TERM} = sub {
        $pm->signal_all_children('TERM');
    };
    kill 'USR1', getppid();
    $pm->wait_all_children;
    exit;
}

sub cmd_to_mgr {
    my ($self,$cmd,$peername,$reqs) = @_;
    my $msg = $cmd . Digest::MD5::md5($peername) . sprintf('%08x',$reqs);
    _sendn(fileno($self->{mgr_sock}), $msg, 0);
}

sub keep_it {
    my ($self,$conn) = @_;
    if ( $conn->{direct} ) {
        $self->cmd_to_mgr("push", $conn->{peername}, $conn->{reqs});
        my $ret;
        do {
            $ret = IO::FDPass::send(fileno $self->{mgr_sock}, fileno $conn->{fh});
            die $! if ( !defined $ret && $! != EAGAIN && $! != EWOULDBLOCK && $! != EINTR);
            #need select?
        } while (!$ret);
    }
    else {
        $self->cmd_to_mgr("keep", $conn->{peername}, $conn->{reqs});
    }
}

sub accept_or_recv {
    my $self = shift;
    my @for_read = @_;
    my $conn;
    use open 'IO' => ':unix';
    for my $sock ( @for_read ) {
        if ( fileno $sock == fileno $self->{listen_sock} ) {
            my ($fh,$peer);
            if ( $have_accept4 ) {
                $peer = Linux::Socket::Accept4::accept4($fh,$self->{listen_sock}, $have_accept4);
            }
            else {
                $peer = accept($fh,$self->{listen_sock});
                fh_nonblocking($fh,1) if $peer;
            }
            if ( !$peer && ($! != EINTR && $! != EAGAIN && $! != EWOULDBLOCK && $! != ESPIPE) ) {
                warn sprintf 'failed to accept: %s (%d)', $!, $!;
                next;
            }
            next unless $peer;
            setsockopt($fh, IPPROTO_TCP, TCP_NODELAY, 1)
                or die "setsockopt(TCP_NODELAY) failed:$!";
            my ($peerport,$peerhost) = unpack_sockaddr_in $peer;
            my $peeraddr = inet_ntoa($peerhost);
            $conn = {
                fh => $fh,
                peername => $peer,
                peerport => $peerport,
                peeraddr => $peeraddr,
                direct => 1,
                reqs => 0,
            };
            last;
        }
        elsif ( fileno $sock == fileno $self->{fdpass_sock}[READER] ) {
            my $fd = IO::FDPass::recv(fileno $self->{fdpass_sock}[READER]);
            if ( $fd < 0 && ($! != EINTR && $! != EAGAIN && $! != EWOULDBLOCK && $! != ESPIPE) ) {
                warn sprintf("could not recv fd: %s (%d)", $!, $!);
            }
            next if $fd <= 0;
            my $peer; 
            if ( _getpeername($fd, $peer) < 0 ) {
                next;
            }
            open(my $fh, '>>&='.$fd)
                or die "could not open fd: $!";
            my ($peerport,$peerhost) = unpack_sockaddr_in $peer;
            my $peeraddr = inet_ntoa($peerhost);
            $conn = {
                fh => $fh,
                peername => $peer,
                peerport => $peerport,
                peeraddr => $peeraddr,
                direct => 0,
                reqs => 1, #xx
            };
            last;
        }
    }
    return unless $conn;
    $conn;
}

my $bad_response = [ 400, [ 'Content-Type' => 'text/plain', 'Connection' => 'close' ], [ 'Bad Request' ] ];
sub handle_connection {
    my($self, $env, $conn, $app, $use_keepalive, $is_keepalive, $prebuf, $reqs) = @_;
    
    my $buf = '';
    my $pipelined_buf='';
    my $res = $bad_response;

    while (1) {
        my $rlen;
        if ( defined $prebuf ) {
            $rlen = length $prebuf;
            $buf = $prebuf;
            undef $prebuf;
        }
        else {
            $rlen = $self->read_timeout(
                $conn, \$buf, MAX_REQUEST_SIZE - length($buf), length($buf),
                $is_keepalive ? $self->{keepalive_timeout} : $self->{timeout},
            ) or return;
        }

        my $reqlen = parse_http_request($buf, $env);
        if ($reqlen >= 0) {
            # handle request
            my $protocol = $env->{SERVER_PROTOCOL}; 
            if ($use_keepalive) {
                if ( $protocol eq 'HTTP/1.1' ) {
                    if (my $c = $env->{HTTP_CONNECTION}) {
                        $use_keepalive = undef 
                            if $c =~ /^\s*close\s*/i;
                    }
                }
                else {
                    if (my $c = $env->{HTTP_CONNECTION}) {
                        $use_keepalive = undef
                            unless $c =~ /^\s*keep-alive\s*/i;
                    } else {
                        $use_keepalive = undef;
                    }
                }
                if ( $use_keepalive && $reqs <= 1 ) {
                    $use_keepalive = $self->can_keepalive;
                }
            }
            $buf = substr $buf, $reqlen;
            my $chunked = do { no warnings; lc delete $env->{HTTP_TRANSFER_ENCODING} eq 'chunked' };
            if (my $cl = $env->{CONTENT_LENGTH}) {
                my $buffer = Plack::TempBuffer->new($cl);
                while ($cl > 0) {
                    my $chunk;
                    if (length $buf) {
                        $chunk = $buf;
                        $buf = '';
                    } else {
                        $self->read_timeout(
                            $conn, \$chunk, $cl, 0, $self->{timeout})
                            or return;
                    }
                    $buffer->print($chunk);
                    $cl -= length $chunk;
                }
                $env->{'psgi.input'} = $buffer->rewind;
            }
            elsif ($chunked) {
                my $buffer = Plack::TempBuffer->new;
                my $chunk_buffer = '';
                my $length;
                DECHUNK: while(1) {
                    my $chunk;
                    if ( length $buf ) {
                        $chunk = $buf;
                        $buf = '';
                    }
                    else {
                        $self->read_timeout($conn, \$chunk, CHUNKSIZE, 0, $self->{timeout})
                            or return;
                    }

                    $chunk_buffer .= $chunk;
                    while ( $chunk_buffer =~ s/^(([0-9a-fA-F]+).*\015\012)// ) {
                        my $trailer   = $1;
                        my $chunk_len = hex $2;
                        if ($chunk_len == 0) {
                            last DECHUNK;
                        } elsif (length $chunk_buffer < $chunk_len + 2) {
                            $chunk_buffer = $trailer . $chunk_buffer;
                            last;
                        }
                        $buffer->print(substr $chunk_buffer, 0, $chunk_len, '');
                        $chunk_buffer =~ s/^\015\012//;
                        $length += $chunk_len;                        
                    }
                }
                $env->{CONTENT_LENGTH} = $length;
                $env->{'psgi.input'} = $buffer->rewind;
                
            } else {
                if ( $buf =~ m!^(?:GET|HEAD)! ) { #pipeline
                    $pipelined_buf = $buf;
                    $use_keepalive = 1;
                } # else clear buffer
                $env->{'psgi.input'} = $null_io;
            }

            if ( $env->{HTTP_EXPECT} ) {
                if ( $env->{HTTP_EXPECT} eq '100-continue' ) {
                    $self->write_all($conn, "HTTP/1.1 100 Continue\015\012\015\012")
                        or return;
                } else {
                    $res = [417,[ 'Content-Type' => 'text/plain', 'Connection' => 'close' ], [ 'Expectation Failed' ] ];
                    last;
                }
            }

            $res = Plack::Util::run_app $app, $env;
            last;
        }
        if ($reqlen == -2) {
            # request is incomplete, do nothing
        } elsif ($reqlen == -1) {
            # error, close conn
            last;
        }
    }

    if (ref $res eq 'ARRAY') {
        $self->_handle_response($env, $res, $conn, \$use_keepalive);
    } elsif (ref $res eq 'CODE') {
        $res->(sub {
            $self->_handle_response($env, $_[0], $conn, \$use_keepalive);
        });
    } else {
        die "Bad response $res";
    }

    return ($use_keepalive, $pipelined_buf);
}

sub _handle_response {
    my($self, $env, $res, $conn, $use_keepalive_r) = @_;
    my $protocol = $env->{SERVER_PROTOCOL};
    my $status_code = $res->[0];
    my $headers = $res->[1];
    my $body = $res->[2];
    
    my @lines;
    my %send_headers;
    for (my $i = 0; $i < @$headers; $i += 2) {
        my $k = $headers->[$i];
        my $v = $headers->[$i + 1];
        my $lck = lc $k;
        if ($lck eq 'connection') {
            $$use_keepalive_r = undef
                if $$use_keepalive_r && lc $v ne 'keep-alive';
        } else {
            push @lines, "$k: $v\015\012";
            $send_headers{$lck} = $v;
        }
    }
    if ( ! exists $send_headers{server} ) {
        unshift @lines, "Server: $self->{server_software}\015\012";
    }
    if ( ! exists $send_headers{date} ) {
        unshift @lines, "Date: @{[HTTP::Date::time2str()]}\015\012";
    }

    # try to set content-length when keepalive can be used, or disable it
    my $use_chunked;
    if ( $protocol eq 'HTTP/1.0' ) {
        if ($$use_keepalive_r) {
            if (defined $send_headers{'content-length'}
                || defined $send_headers{'transfer-encoding'}) {
                # ok
            }
            elsif ( ! Plack::Util::status_with_no_entity_body($status_code)
                    && defined(my $cl = Plack::Util::content_length($body))) {
                push @lines, "Content-Length: $cl\015\012";
            }
            else {
                $$use_keepalive_r = undef
            }            
        }
        push @lines, "Connection: keep-alive\015\012" if $$use_keepalive_r;
        push @lines, "Connection: close\015\012" if !$$use_keepalive_r; #fmm..
    }
    elsif ( $protocol eq 'HTTP/1.1' ) {
        if (defined $send_headers{'content-length'}
            || defined $send_headers{'transfer-encoding'}) {
            # ok
        } elsif ( !Plack::Util::status_with_no_entity_body($status_code) ) {
            push @lines, "Transfer-Encoding: chunked\015\012";
            $use_chunked = 1;
        }
        push @lines, "Connection: close\015\012" unless $$use_keepalive_r;

    }

    unshift @lines, "HTTP/1.1 $status_code @{[ HTTP::Status::status_message($status_code) ]}\015\012";
    push @lines, "\015\012";
    
    if (defined $body && ref $body eq 'ARRAY' && @$body == 1
            && length $body->[0] < 8192) {
        # combine response header and small request body
        my $buf = $body->[0];
        if ($use_chunked ) {
            my $len = length $buf;
            $buf = sprintf("%x",$len) . "\015\012" . $buf . "\015\012" . '0' . "\015\012\015\012";
        }
        my $len = $self->write_all(
            $conn, join('', @lines, $buf), $self->{timeout},
        );
        warn $! unless $len;
        return;
    }

    if ( $have_sendfile && !$use_chunked
      && defined $body && ref $body ne 'ARRAY'
      && fileno($body) ) {
        my $cl = $send_headers{'content-length'} || -s $body;
        # sendfile
        my $use_cork = 0;
        if ( $^O eq 'linux' ) {
            setsockopt($conn, IPPROTO_TCP, 3, 1)
                and $use_cork = 1;
        }
        $self->write_all($conn, join('', @lines), $self->{timeout})
            or return;
        my $len = $self->sendfile_all($conn, $body, $cl, $self->{timeout});
        #warn sprintf('%d:%s',$!, $!) unless $len;
        if ( $use_cork && $$use_keepalive_r ) {
            setsockopt($conn, IPPROTO_TCP, 3, 0);
        }
        return;
    }

    $self->write_all($conn, join('', @lines), $self->{timeout})
        or return;

    if (defined $body) {
        my $failed;
        my $completed;
        my $body_count = (ref $body eq 'ARRAY') ? $#{$body} + 1 : -1;
        Plack::Util::foreach(
            $body,
            sub {
                unless ($failed) {
                    my $buf = $_[0];
                    --$body_count;
                    if ( $use_chunked ) {
                        my $len = length $buf;
                        return unless $len;
                        $buf = sprintf("%x",$len) . "\015\012" . $buf . "\015\012";
                        if ( $body_count == 0 ) {
                            $buf .= '0' . "\015\012\015\012";
                            $completed = 1;
                        }
                    }
                    $self->write_all($conn, $buf, $self->{timeout})
                        or $failed = 1;
                }
            },
        );
        $self->write_all($conn, '0' . "\015\012\015\012", $self->{timeout}) if $use_chunked && !$completed;
    } else {
        return Plack::Util::inline_object
            write => sub {
                my $buf = $_[0];
                if ( $use_chunked ) {
                    my $len = length $buf;
                    return unless $len;
                    $buf = sprintf("%x",$len) . "\015\012" . $buf . "\015\012"
                }
                $self->write_all($conn, $buf, $self->{timeout})
            },
            close => sub {
                $self->write_all($conn, '0' . "\015\012\015\012", $self->{timeout}) if $use_chunked;
            };
    }
}

sub _calc_minmax_per_child {
    my $self = shift;
    my ($max,$min) = @_;
    if (defined $min) {
        srand((rand() * 2 ** 30) ^ $$ ^ time);
        return $max - int(($max - $min + 1) * rand);
    } else {
        return $max;
    }
}

# returns value returned by $cb, or undef on timeout or network error
sub do_io {
    my ($self, $is_write, $sock, $buf, $len, $off, $timeout) = @_;
    my $ret;
    unless ($is_write || delete $self->{_is_deferred_accept}) {
        goto DO_SELECT;
    }
 DO_READWRITE:
    # try to do the IO
    if ($is_write && $is_write == 1) {
        $ret = syswrite $sock, $buf, $len, $off
            and return $ret;
    } elsif ($is_write && $is_write == 2) {
        $ret = Sys::Sendfile::sendfile($sock, $buf, $len)
            and return $ret;
        $ret = undef if defined $ret && $ret == 0 && $! == EAGAIN; #hmm
    } else {
        $ret = sysread $sock, $$buf, $len, $off
            and return $ret;
    }
    unless ((! defined($ret)
                 && ($! == EINTR || $! == EAGAIN || $! == EWOULDBLOCK))) {
        return;
    }
    # wait for data
 DO_SELECT:
    while (1) {
        my ($rfd, $wfd);
        my $efd = '';
        vec($efd, fileno($sock), 1) = 1;
        if ($is_write) {
            ($rfd, $wfd) = ('', $efd);
        } else {
            ($rfd, $wfd) = ($efd, '');
        }
        my $start_at = time;
        my $nfound = select($rfd, $wfd, $efd, $timeout);
        $timeout -= (time - $start_at);
        last if $nfound;
        return if $timeout <= 0;
    }
    goto DO_READWRITE;
}

sub sendfile_timeout {
    my ($self, $sock, $fh, $len, $off, $timeout) = @_;
    $self->do_io(2, $sock, $fh, $len, $off, $timeout);
}

sub sendfile_all {
    my ($self, $sock, $fh, $cl, $timeout) = @_;
    my $off = 0;
    while (my $len = $cl - $off) {
        my $ret = $self->sendfile_timeout($sock, $fh, $len, $off, $timeout)
            or return;
        $off += $ret;
        seek($fh, $off, 0) if $cl != $off;
    }
    return $cl;
}


#STATIC
sub write_pid {
    my $file = shift;
    if (-e $file) {
        open PID, $file;
        my $pid = <PID>;
        close PID;
        if (kill 0, $pid) {
            croak "Can't rewrite pid of alive process\n";
        }
    }

    open PID, '>', $file or croak "Can't open pid file: $file\n";
    print PID $$;
    close PID;
    return 1;
}

1;
