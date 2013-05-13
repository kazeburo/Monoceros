package Monoceros::Server;

use strict;
use warnings;
use base qw/Plack::Handler::Starlet/;
use IO::Socket;
use IO::Select;
use IO::FDPass;
use Parallel::Prefork;
use AnyEvent;
use AnyEvent::Util qw(fh_nonblocking);
use Digest::MD5 qw/md5_hex/;
use Time::HiRes qw/time/;
use Carp ();
use Plack::TempBuffer;
use Plack::Util;
use Plack::HTTPParser qw( parse_http_request );
use POSIX qw(EINTR EAGAIN EWOULDBLOCK :sys_wait_h);
use Socket qw(IPPROTO_TCP TCP_NODELAY);

use constant WRITER => 0;
use constant READER => 1;

use constant S_SOCK => 0;
use constant S_TIME => 1;
use constant S_REQS => 2;
use constant S_IDLE => 3;

use constant MAX_REQUEST_SIZE => 131072;
my $null_io = do { open my $io, "<", \""; $io };

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

    my $self = bless {
        host                 => $args{host} || 0,
        port                 => $args{port} || 8080,
        max_workers          => $max_workers,
        timeout              => $args{timeout} || 300,
        keepalive_timeout    => $args{keepalive_timeout} || 10,
        max_keepalive_reqs   => $args{max_keepalive_reqs} || 100,
        server_software      => $args{server_software} || $class,
        server_ready         => $args{server_ready} || sub {},
        min_reqs_per_child   => (
            defined $args{min_reqs_per_child}
                ? $args{min_reqs_per_child} : undef,
        ),
        max_reqs_per_child   => (
            $args{max_reqs_per_child} || $args{max_requests} || 100,
        ),
        err_respawn_interval => (
            defined $args{err_respawn_interval}
                ? $args{err_respawn_interval} : undef,
        ),
        _using_defer_accept  => 0,
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
    my @worker_pipe = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, 0)
        or die "failed to create socketpair: $!";
    $self->{worker_pipe} = \@worker_pipe; 

    my @defer_pipe = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, 0)
        or die "failed to create socketpair: $!";
    $self->{defer_pipe} = \@defer_pipe; 

    my @lstn_pipe = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, 0)
            or die "failed to create socketpair: $!";    
    $self->{lstn_pipe} = \@lstn_pipe;

    1;
}

sub run_workers {
    my ($self,$app) = @_;
    local $SIG{PIPE} = 'IGNORE';    
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
    my $info = shift;

    $info->[S_IDLE] = 0; #no-idle

    $self->{fdsend_queue} ||= [];
    push @{$self->{fdsend_queue}},  $info;
    $self->{fdsend_worker} ||= AE::io $self->{lstn_pipe}[WRITER], 1, sub {
        do {
            if ( !$self->{fdsend_queue}[S_SOCK] ) {
                shift @{$self->{fdsend_queue}};
                return;
            }
            if ( ! IO::FDPass::send(fileno $self->{lstn_pipe}[WRITER], fileno $self->{fdsend_queue}[0][S_SOCK] ) ) {
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

    $self->{lstn_pipe}[READER]->close;
    fh_nonblocking $self->{lstn_pipe}[WRITER], 1;
    $self->{worker_pipe}->[WRITER]->close;
    fh_nonblocking $self->{worker_pipe}->[READER], 1;
    $self->{defer_pipe}->[WRITER]->close;
    fh_nonblocking $self->{defer_pipe}->[READER], 1;
    fh_nonblocking $self->{listen_sock}, 1;

    my %manager;
    my %sockets;
    my $term_received = 0;
    my %wait_read;

    my $cv = AE::cv;
    my $sig;$sig = AE::signal 'TERM', sub {
        $term_received++;
        kill 'USR1', $worker_pid; #stop accept
        my $t;$t = AE::timer 0, 1, sub {
            my $time = time;
            for my $key ( keys %sockets ) {
                if ( !$sockets{$key}->[S_IDLE] && $time - $sockets{$key}->[1] > $self->{keepalive_timeout} ) {
                    delete $wait_read{$key};
                    delete $sockets{$key};                
                }
            }
            return if keys %sockets;
            undef $t;
            kill 'TERM', $worker_pid; #stop process
            $cv->send;
        };
    };

    $manager{disconnect_keepalive_timeout} = AE::timer 0, 1, sub {
        my $time = time;
        for my $key ( keys %sockets ) {
            if ( !$sockets{$key}->[S_IDLE] && (!$sockets{$key}->[S_SOCK] || !$sockets{$key}->[S_SOCK]->connected()) ) {
                delete $wait_read{$key};
                delete $sockets{$key};
            }
            elsif ( $sockets{$key}->[S_IDLE] && $sockets{$key}->[S_REQS] == 0 
                     && $time - $sockets{$key}->[1] > $self->{timeout} ) { #idle && first req 
                delete $wait_read{$key};
                delete $sockets{$key};
            }
            elsif ( $sockets{$key}->[S_IDLE] && $sockets{$key}->[S_REQS] > 0 &&
                     $time - $sockets{$key}->[1] > $self->{keepalive_timeout} ) { #idle && keepalive
                delete $wait_read{$key};
                delete $sockets{$key};
            }
        }
    };
    
    if ( $self->{_using_defer_accept} ) {
        $manager{defer_listener} = AE::io $self->{defer_pipe}->[READER], 0, sub {
            my @fd;
            D_PIPE_READ: for (1..$self->{max_workers}) {
                my $fd = IO::FDPass::recv($self->{defer_pipe}->[READER]->fileno);
                last D_PIPE_READ if $! == Errno::EAGAIN || $! == Errno::EWOULDBLOCK;
                next if $fd < 0;
                push @fd, $fd;
            }
            for my $fd ( @fd ) {
                my $fh = IO::Socket::INET->new_from_fd($fd,'r+')
                    or die "unable to convert file descriptor to handle: $!";
                my $peername = $fh->peername;
                next unless $peername;
                my $remote = md5_hex($peername);
                $sockets{$remote} = [$fh,time,1,1];  #fh,time,reqs,idle
                $wait_read{$remote} = AE::io $sockets{$remote}->[S_SOCK], 0, sub {
                    undef $wait_read{$remote};
                    if ( !$sockets{$remote}->[S_SOCK] || !$sockets{$remote}->[S_SOCK]->connected()) {
                        delete $sockets{$remote};
                        return;
                    }
                    $self->queued_fdsend($sockets{$remote});
                };
            }
        };
    }
    else {
        $manager{main_listener} = AE::io $self->{listen_sock}, 0, sub {
            L_SOCK_READ: for (1..$self->{max_workers}) {
                return if $term_received;
                my ($fh,$peer) = $self->{listen_sock}->accept;
                last L_SOCK_READ if $! == Errno::EAGAIN || $! == Errno::EWOULDBLOCK;
                next unless $fh;
                my $remote = md5_hex($peer);
                $sockets{$remote} = [$fh,time,0,1];  #fh,time,reqs,idle
                fh_nonblocking $fh, 1
                    or die "failed to set socket to nonblocking mode:$!";
                setsockopt($fh, IPPROTO_TCP, TCP_NODELAY, 1)
                    or die "setsockopt(TCP_NODELAY) failed:$!";
                $wait_read{$remote} = AE::io $fh, 0, sub {
                    undef $wait_read{$remote};
                    if ( !$sockets{$remote}->[S_SOCK] || !$sockets{$remote}->[S_SOCK]->connected()) {
                        delete $sockets{$remote};
                        return;
                    }
                    $self->queued_fdsend($sockets{$remote});
                };
            }
        };
    }
 
   my $pipe_buf = '';
    $manager{worker_listener} = AE::io $self->{worker_pipe}->[READER], 0, sub {
        my @keep;
        PIPE_READ: for (1..$self->{max_workers}) {
            my $len = $self->{worker_pipe}->[READER]->sysread($pipe_buf, 10240);
            last PIPE_READ if $! == Errno::EAGAIN || $! == Errno::EWOULDBLOCK;
            BUF_READ: while ( length $pipe_buf ) {
                my $string = substr $pipe_buf, 0, 37, '';
                my ($method,$remote) = split / /,$string, 2;
                next BUF_READ unless exists $sockets{$remote};
                if ( $method eq 'exit' ) {
                    $sockets{$remote}->[S_IDLE] = 1; #idle
                    delete $sockets{$remote};
                } elsif ( $method eq 'keep') {
                    push @keep, $remote;
                }
                last BUF_READ if length $pipe_buf < 37;
            }
        }

        my $time = time;
        for my $remote ( @keep ) {
            $sockets{$remote}->[S_TIME] = $time; #time
            $sockets{$remote}->[S_REQS]++; #reqs
            $sockets{$remote}->[S_IDLE] = 1; #idle
            $wait_read{$remote} = AE::io $sockets{$remote}->[S_SOCK], 0, sub {
                undef $wait_read{$remote};
                if ( !$sockets{$remote}->[S_SOCK] || !$sockets{$remote}->[S_SOCK]->connected()) {
                    delete $sockets{$remote};
                    return;
                }
                $self->queued_fdsend($sockets{$remote});
            };
        }

    };

    $cv->recv;
    \%manager;
}

sub request_worker {
    my ($self,$app) = @_;

    if ( $self->{_using_defer_accept} ) {
        $self->{listen_sock}->blocking(0);
    }
    else {
        $self->{listen_sock}->close;
    }

    $self->{worker_pipe}->[READER]->close;
    $self->{defer_pipe}->[READER]->close;
    $self->{lstn_pipe}[WRITER]->close;
    $self->{lstn_pipe}[READER]->blocking(0);

    # use Parallel::Prefork
    my %pm_args = (
        max_workers => $self->{max_workers},
        trap_signals => {
            TERM => 'TERM',
            HUP  => 'TERM',
            USR1 => 'TERM',
        },
    );
    if (defined $self->{err_respawn_interval}) {
        $pm_args{err_respawn_interval} = $self->{err_respawn_interval};
    }

    my $pm = Parallel::Prefork->new(\%pm_args);

    while ($pm->signal_received !~ /^(TERM)$/) {
        $pm->start(sub {
            srand();
            my %sys_fileno;
            my $select = IO::Select->new();
            $sys_fileno{$self->{lstn_pipe}[READER]->fileno} = 1;
            $select->add($self->{lstn_pipe}[READER]);
            if ( $self->{_using_defer_accept} ) {
                $sys_fileno{$self->{listen_sock}->fileno} = 1;
                $select->add($self->{listen_sock});
            }

            my $max_reqs_per_child = $self->_calc_reqs_per_child();
            my $proc_req_count = 0;
            $self->{can_exit} = 1;

            $self->{term_received} = 0;
            local $SIG{TERM} = sub {
                $self->{term_received}++;
                exit 0 if $self->{term_received} > 1;
                
            };

            local $SIG{PIPE} = 'IGNORE';

            my $next_conn;

            while ( $self->{term_received} == 1 || $proc_req_count < $max_reqs_per_child ) {
                my $conn;
                if ( $next_conn ) {
                    $conn = $next_conn;
                    $next_conn = undef;
                }
                else {
                    my @can_read = $select->can_read(1);
                    if ( !@can_read ) {
                        next;
                    }
                    for (@can_read) {
                        if ( ! exists $sys_fileno{$_->fileno} ) {
                            $select->remove($_);
                        }
                    }
                    $conn = $self->accept_or_recv( grep { exists $sys_fileno{$_->fileno} } @can_read );
                }
                next unless $conn;
                
                ++$proc_req_count;
                my ($peerport,$peerhost) = unpack_sockaddr_in $conn->{peername};
                my $remote = md5_hex($conn->{peername});
                my $env = {
                    SERVER_PORT => $self->{port},
                    SERVER_NAME => $self->{host},
                    SCRIPT_NAME => '',
                    REMOTE_ADDR => inet_ntoa($peerhost),
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
                    'psgix.io'          => $conn->{fh},
                };
                $self->{_is_deferred_accept} = 1; #ready to read
                my $prebuf;
                if ( exists $conn->{buf} ) {
                    $prebuf = delete $conn->{buf};
                }
                elsif ( $conn->{direct} ) {
                    my $ret = $conn->{fh}->sysread($prebuf, MAX_REQUEST_SIZE);
                    if ( ! defined $ret && ($! == EAGAIN || $! == EWOULDBLOCK) ) {
                        $select->add($conn->{fh});
                        IO::FDPass::send($self->{defer_pipe}->[WRITER]->fileno, $conn->{fn});
                        next;
                    }
                }
                my $may_keepalive = $self->{term_received} == 0;
                my $is_keepalive = 1; # to use "keepalive_timeout" in handle_connection, 
                                      #  treat every connection as keepalive
                
                my $keepalive = $self->handle_connection($env, $conn->{fh}, $app, 
                                                         $may_keepalive, $is_keepalive, $prebuf);
                if ( !$keepalive ) {
                    $self->{worker_pipe}->[WRITER]->syswrite("exit $remote")
                        unless $conn->{direct};
                    next;
                }

                # read fowrard
                if ( $select->count() <= scalar(keys  %sys_fileno) + $self->{max_workers} ) {
                    $next_conn = $self->accept_or_recv( 
                        $self->{_using_defer_accept} ? ($self->{lstn_pipe}[READER], $self->{listen_sock}) : ($self->{lstn_pipe}[READER])
                    );
                    if ( ! $next_conn ) {
                        my $ret = $conn->{fh}->sysread(my $buf, MAX_REQUEST_SIZE);
                        # readed next req
                        if ( defined $ret && $ret > 0 ) {
                            $next_conn = $conn;
                            $next_conn->{buf} = $buf;
                        }
                    }
                }
                # wait if !next_conn and ! defined next_buf
                if ( !$next_conn  || ( $next_conn && $next_conn->{fn} != $conn->{fn}) ) {
                    if ( $conn->{direct} ) {
                        $select->add($conn->{fh});
                        IO::FDPass::send($self->{defer_pipe}->[WRITER]->fileno, $conn->{fn});
                    }
                    else {
                        $self->{worker_pipe}->[WRITER]->syswrite("keep $remote");
                    }
                }
            }
        });
    }
    $pm->wait_all_children;
    exit;
}

sub accept_or_recv {
    my $self = shift;
    my @for_read = @_;
    my $conn;
    for my $pipe_or_sock ( @for_read ) {
        if ( $self->{_using_defer_accept} && $pipe_or_sock->fileno eq $self->{listen_sock}->fileno ) {
            my ($fh,$peer) = $self->{listen_sock}->accept;
            next unless $fh;
            $fh->blocking(0);
            setsockopt($fh, IPPROTO_TCP, TCP_NODELAY, 1)
                or die "setsockopt(TCP_NODELAY) failed:$!";
            $conn = {
                fh => $fh,
                fn => $fh->fileno,
                peername => $peer,
                direct => 1,
                reqs => 0,
            };
            last;
        }
        elsif ( $pipe_or_sock->fileno eq $self->{lstn_pipe}[READER]->fileno ) {
            my $fd = IO::FDPass::recv($pipe_or_sock->fileno);
            if ( $fd >= 0 ) {
                my $fh = IO::Socket::INET->new_from_fd($fd,'r+')
                    or die "unable to convert file descriptor to handle: $!";
                $conn = {
                    fh => $fh,
                    fn => $fh->fileno,
                    peername => $fh->peername,
                    direct => 0,
                    reqs => 0,
                };
                last;
            }
        }
    }
    return unless $conn;
    return unless $conn->{fh};
    return unless $conn->{peername};
    $conn;
}

sub handle_connection {
    my($self, $env, $conn, $app, $use_keepalive, $is_keepalive, $prebuf) = @_;
    
    my $buf = '';
    my $res = [ 400, [ 'Content-Type' => 'text/plain' ], [ 'Bad Request' ] ];
    
    local $self->{can_exit} = 1;
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
        $self->{can_exit} = 0;
        my $reqlen = parse_http_request($buf, $env);
        if ($reqlen >= 0) {
            # handle request
            if ($use_keepalive) {
                if (my $c = $env->{HTTP_CONNECTION}) {
                    $use_keepalive = undef
                        unless $c =~ /^\s*keep-alive\s*/i;
                } else {
                    $use_keepalive = undef;
                }
            }
            $buf = substr $buf, $reqlen;
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
            } else {
                $env->{'psgi.input'} = $null_io;
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
        $self->_handle_response($res, $conn, \$use_keepalive);
    } elsif (ref $res eq 'CODE') {
        $res->(sub {
            $self->_handle_response($_[0], $conn, \$use_keepalive);
        });
    } else {
        die "Bad response $res";
    }

    return $use_keepalive;
}

1;
