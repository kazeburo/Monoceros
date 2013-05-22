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
use POSIX qw(EINTR EAGAIN EWOULDBLOCK ESPIPE EDOM :sys_wait_h);
use Socket qw(IPPROTO_TCP TCP_NODELAY);
use File::Temp qw/tempfile/;

use constant WRITER => 0;
use constant READER => 1;

use constant S_SOCK => 0;
use constant S_TIME => 1;
use constant S_REQS => 2;
use constant S_IDLE => 3;

use constant MAX_REQUEST_SIZE => 131072;
use constant CHUNKSIZE    => 64 * 1024;

use constant DEBUG        => $ENV{MONOCEROS_DEBUG} || 0;

my $null_io = do { open my $io, "<", \""; $io };
my %ok_accept_errno = map { $_ => 1 } (EAGAIN, EWOULDBLOCK, ESPIPE, EINTR);
my %ok_recv_errno = map { $_ => 1 } (EAGAIN, EWOULDBLOCK, EINTR, EDOM);

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

    my $open_max = eval { POSIX::sysconf (POSIX::_SC_OPEN_MAX ()) - 1 } || 1023;

    my $self = bless {
        host                 => $args{host} || 0,
        port                 => $args{port} || 8080,
        max_workers          => $max_workers,
        timeout              => $args{timeout} || 300,
        keepalive_timeout    => $args{keepalive_timeout} || 10,
        max_keepalive_connection => $args{max_keepalive_connection} || int($open_max/2),
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

    my ($fh, $filename) = tempfile('monoceros_internal_XXXXXX',UNLINK => 0, SUFFIX => '.sock', TMPDIR => 1);
    close($fh);
    unlink($filename);
    $self->{worker_sock} = $filename;
    my $sock = IO::Socket::UNIX->new(
        Type => SOCK_STREAM,
        Listen => Socket::SOMAXCONN(),
        Local => $filename,
    ) or die $!;
    $self->{internal_server} = $sock;

    my @lstn_pipe = IO::Socket->socketpair(AF_UNIX, SOCK_STREAM, 0)
            or die "failed to create socketpair: $!";
    $self->{lstn_pipe} = \@lstn_pipe;

    1;
}

sub run_workers {
    my ($self,$app) = @_;
    local $SIG{PIPE} = 'IGNORE';    
    my $pid = fork;  
    if ( $pid ) {
        #parent
        $self->connection_manager($pid);
    }
    elsif ( defined $pid ) {
        $self->request_worker($app);
        exit;
    }
    else {
        die "failed fork:$!";
    }
}

sub queued_fdsend {
    my $self = shift;
    my $fd = shift;

    if ( ! exists $self->{sockets}{$fd} ) {
        return;
    }
    $self->{sockets}{$fd}[S_IDLE] = 0;

    push @{$self->{fdsend_queue}},  $fd;
    $self->{fdsend_worker} ||= AE::io $self->{lstn_pipe}[WRITER], 1, sub {
        while ( my $fd = shift @{$self->{fdsend_queue}} ) {
            if ( ! exists $self->{sockets}{$fd}  ) {
                next;
            }
            if ( !$self->{sockets}{$fd}[S_SOCK] || !$self->{sockets}{$fd}[S_SOCK]->connected ) {
                delete $self->{sockets}{$fd};
                next;
            }
            if ( ! IO::FDPass::send(fileno $self->{lstn_pipe}[WRITER], $fd ) ) {
                if ( $! == Errno::EAGAIN || $! == Errno::EWOULDBLOCK ) {
                    unshift @{$self->{fdsend_queue}}, $fd;
                    return;
                }
                die "unable to pass file handle: $!"; 
                undef $self->{fdsend_worker};
            }
        } 
        undef $self->{fdsend_worker};
    };
    1;
}

sub connection_manager {
    my ($self, $worker_pid) = @_;

    $self->{lstn_pipe}[READER]->close;
    fh_nonblocking $self->{lstn_pipe}[WRITER], 1;
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
            my $queued = scalar @{$self->{fdsend_queue}};
            my $active = scalar grep { !$self->{sockets}{$_}[S_IDLE] } keys %{$self->{sockets}};
            my $idle = scalar grep { $self->{sockets}{$_}[S_IDLE] } keys %{$self->{sockets}};
            warn "queue: $queued | total: $total | active: $active | idle: $idle";
        }
        for my $key ( keys %{$self->{sockets}} ) { #key = fd
            if ( ! $self->{sockets}{$key}[S_IDLE] && $time - $self->{sockets}{$key}[S_TIME] > $self->{timeout}
                     && (!$self->{sockets}{$key}[S_SOCK] || !$self->{sockets}{$key}[S_SOCK]->connected()) ) {
                delete $wait_read{$key};
                delete $self->{sockets}{$key};
            }
            elsif ( $self->{sockets}{$key}[S_IDLE] && $self->{sockets}{$key}[S_REQS] == 0 
                     && $time - $self->{sockets}{$key}[S_TIME] > $self->{timeout} ) { #idle && first req 
                delete $wait_read{$key};
                delete $self->{sockets}{$key};
            }
            elsif ( $self->{sockets}{$key}[S_IDLE] && $self->{sockets}{$key}[S_REQS] > 0 &&
                     $time - $self->{sockets}{$key}[S_TIME] > $self->{keepalive_timeout} ) { #idle && keepalive
                delete $wait_read{$key};
                delete $self->{sockets}{$key};
            }
        }
    };

    $manager{internal_server} = AE::io $self->{internal_server}, 0, sub {
        my $sock = $self->{internal_server}->accept;
        return unless $sock;
        my $buf = '';
        my $state = 'cmd';
        my $ws; $ws = AE::io $sock, 0, sub {
            if ( $state eq 'cmd' ) {
                my $len = sysread($sock, $buf, 37 - length($buf), length($buf));
                if ( defined $len && $len == 0 ) {
                    undef $ws;
                    return;
                }
                return if length $buf < 37;
                my $string = substr $buf, 0, 37, '';
                my ($method,$remote) = split / /,$string, 2;

                ## cmd
                # stat
                if ( $method eq 'stat' ) {
                    my $msg = ( scalar keys %{$self->{sockets}} < $self->{max_keepalive_connection} ) ? 'OK' : 'NP';
                    $self->write_all_aeio($sock, $msg, $self->{keepalive_timeout});
                    return;
                }                
                # send
                if ( $method eq 'send' ) {
                    $state = 'fd';
                    return;
                }

                return unless exists $hash2fd{$remote};
                my $fd = $hash2fd{$remote};
                if ( ! exists $self->{sockets}{$fd} ) {
                    delete $hash2fd{$remote};
                    return;
                }

                if ( $method eq 'exit' ) { # exit
                    $self->{sockets}{$fd}[S_IDLE] = 1;
                    delete $hash2fd{$remote};
                    delete $self->{sockets}{$fd};
                } elsif ( $method eq 'keep') { #keep
                    $self->{sockets}{$fd}[S_TIME] = time; #time
                    $self->{sockets}{$fd}[S_REQS]++; #reqs
                    $self->{sockets}{$fd}[S_IDLE] = 1; #idle
                    $wait_read{$fd} = AE::io $self->{sockets}{$fd}[S_SOCK], 0, sub {
                        delete $wait_read{$fd};
                        $self->queued_fdsend($fd);
                    };
                }
            }
            elsif ( $state eq 'fd' ) {
                my $fd = IO::FDPass::recv(fileno $sock);
                return if $! == Errno::EAGAIN || $! == Errno::EWOULDBLOCK;
                $state = 'cmd';
                
                if ( $fd < 0 ) {
                    warn sprintf 'Failed recv fd: %s (%d)', $!, $!;
                    $self->write_all_aeio($sock, "NG", $self->{keepalive_timeout});
                    return;
                }
                my $fh = IO::Socket::INET->new_from_fd($fd,'r+');
                if ( !$fh ) {
                    warn "unable to convert file descriptor to handle: $!";
                    $self->write_all_aeio($sock, "NG", $self->{keepalive_timeout});
                    return;
                }
                my $msg = ( scalar keys %{$self->{sockets}} < $self->{max_keepalive_connection} ) ? 'OK' : 'NP';
                $self->write_all_aeio($sock, $msg, $self->{keepalive_timeout});

                my $peername = $fh->peername;
                return unless $peername;
                my $remote = md5_hex($peername);
                $self->{sockets}{$fd} = [$fh,time,1,1];  #fh,time,reqs,idle,buf
                $hash2fd{$remote} = $fd;
                $wait_read{$fd} = AE::io $self->{sockets}{$fd}[S_SOCK], 0, sub {
                    delete $wait_read{$fd};
                    $self->queued_fdsend($fd);
                };
            } # cmd
        } # AE::io
    };

    $cv->recv;
}

sub write_aeio {
    my ($self, $sock, $msg, $len, $off, $timeout) = @_;
 DO_WRITE:
    my $ret = syswrite($sock, $msg, $len, $off);
    return $ret if $ret;
    return if ( ! defined $ret 
                    && ($! != EINTR && $! != EAGAIN || $! != EWOULDBLOCK) );
    my $wcv = AE::cv;
    my $wo; $wo = AE::io $sock, 1, sub {
        undef $wo;
        $wcv->send(1);
    };
    my $wt; $wt = AE::timer 0, $timeout, sub {
        undef $wo;
        undef $wt;
        $wcv->send(1);
    };
    my $can_read = $wcv->recv();
    return unless $can_read;
    goto DO_WRITE;
}

sub write_all_aeio {
    my ($self, $sock, $msg, $timeout) = @_;
    my $off = 0;
    while ( my $len = length($msg) - $off )  {
        my $ret = $self->write_aeio($sock, $msg, $len, $off, $timeout)
            or last;
        $off += $ret;
    }
    return length($msg);
}

sub request_worker {
    my ($self,$app) = @_;

    delete $self->{internal_server};
    $self->{listen_sock}->blocking(0);
    $self->{lstn_pipe}[WRITER]->close;
    $self->{lstn_pipe}[READER]->blocking(0);

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

    my $pm = Parallel::Prefork->new(\%pm_args);

    while ($pm->signal_received !~ /^(?:TERM|USR1)$/) {
        $pm->start(sub {
            srand();
            my %sys_fileno = (
                $self->{lstn_pipe}[READER]->fileno => 1,
                $self->{listen_sock}->fileno => 1,
            );
            $self->{select} = IO::Select->new(
                $self->{lstn_pipe}[READER],
                $self->{listen_sock}
            );

            $self->{mgr_sock} = IO::Socket::UNIX->new(
                Type => SOCK_STREAM,
                Peer => $self->{worker_sock},
            ) or die "$!";
            $self->{mgr_sock}->syswrite("stat "."x"x32, 37);
            $self->{mgr_sock}->sysread(my $buf, 2);
           $self->{stop_keepalive} = ($buf && $buf eq 'OK') ? 0 : time + $self->{keepalive_timeout} + 1; 
            $self->{mgr_sock}->blocking(0);
            
            my $max_reqs_per_child = $self->_calc_reqs_per_child();
            my $proc_req_count = 0;
            
            $self->{term_received} = 0;
            $self->{stop_accept} = 0;
            $self->{stop_keepalive} = 0;
            local $SIG{TERM} = sub {
                $self->{term_received}++;
                exit 0 if $self->{term_received} > 1;
            };
            local $SIG{USR1} = sub {
                $self->{select}->remove($self->{listen_sock});
                $self->{stop_accept}++;
            };
            local $SIG{PIPE} = 'IGNORE';

            my $next_conn;

            while ( $next_conn || $self->{stop_accept} || $proc_req_count < $max_reqs_per_child ) {
                last if ( $self->{term_received} 
                       && !$next_conn 
                   );
                my $conn;
                if ( $next_conn && $next_conn->{buf} ) { #forward read or pipeline
                    $conn = $next_conn;
                }
                else {
                    my @can_read = $self->{select}->can_read(1);
                    for (@can_read) {
                        if ( $next_conn && $_->fileno == $next_conn->{fn} ) {
                            $conn = $next_conn;
                        }
                    }
                    #forward read. but still cannot read
                    $self->keep_or_send($next_conn) if $next_conn && !$conn;

                    #accept or recv
                    $conn = $self->accept_or_recv( grep { exists $sys_fileno{$_->fileno} } @can_read )
                        unless $conn;
                }
                $self->{select}->remove($next_conn->{fh}) if $next_conn;
                $next_conn = undef;
                next unless $conn;
                
                ++$proc_req_count;
                my ($peerport,$peerhost) = unpack_sockaddr_in $conn->{peername};
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
                else {
                    #pre-read
                    my $ret = $conn->{fh}->sysread($prebuf, MAX_REQUEST_SIZE);
                    if ( ! defined $ret && ($! == EAGAIN || $! == EWOULDBLOCK) 
                             && !$self->{stop_keepalive} ) {
                        $self->keep_or_send($conn);
                        next;
                    }
                    elsif ( defined $ret && $ret == 0 ) {
                        #closed?
                        $self->cmd_to_mgr("exit", $conn->{md5}) unless $conn->{direct};
                        next;
                    }
                }
                # stop keepalive if SIG{TERM} or SIG{USR1}. but go-on if pipline req
                my $may_keepalive = 1;
                $may_keepalive = 0 if ($self->{term_received} || $self->{stop_accept} || 
                                           ($self->{stop_keepalive} > time && $conn->{direct} ));
                my $is_keepalive = 1; # to use "keepalive_timeout" in handle_connection, 
                                      #  treat every connection as keepalive
                
                my ($keepalive,$pipelined_buf) = $self->handle_connection($env, $conn->{fh}, $app, 
                                                         $may_keepalive, $is_keepalive, $prebuf);
                if ( !$keepalive ) {
                    $self->cmd_to_mgr("exit",$conn->{md5}) unless $conn->{direct};
                    next;
                }

                # pipeline
                if ( defined $pipelined_buf && length $pipelined_buf ) {
                    $next_conn = $conn;
                    $next_conn->{buf} = $pipelined_buf;
                    next;
                }

                # read fowrard
                if ( $proc_req_count < $max_reqs_per_child ) {
                    my $ret = $conn->{fh}->sysread(my $buf, MAX_REQUEST_SIZE);
                    if ( defined $ret && $ret > 0 ) {
                        $next_conn = $conn;
                        $next_conn->{buf} = $buf;
                        next;
                    }
                    elsif ( defined $ret && $ret == 0 ) {
                        #closed?
                        $self->cmd_to_mgr("exit", $conn->{md5}) unless $conn->{direct};
                        next;
                    }
                    $self->{select}->add($conn->{fh});
                    $next_conn = $conn;
                    next;
                }

                # wait
                $self->keep_or_send($conn);
            }
            
        });
    }
    local $SIG{TERM} = sub {
        $pm->signal_all_children('TERM');
    };
    kill 'USR1', getppid();
    $pm->wait_all_children;
    exit;
}

sub cmd_to_mgr {
    my $self = shift;
    my $msg = join " ", @_;
    $self->write_all($self->{mgr_sock}, $msg, $self->{timeout}) or die $!;
}

sub keep_or_send {
    my ($self,$conn) = @_;
    if ( $conn->{direct} ) {
        $self->cmd_to_mgr("send",$conn->{md5});
        my $ret;
        do {
            $ret = IO::FDPass::send($self->{mgr_sock}->fileno, $conn->{fn});
            die $! if ( !defined $ret && $! != EAGAIN && $! != EWOULDBLOCK);
        } while (!$ret);
        my $buf = '';
        my $to_read = 2;
        $self->{_is_deferred_accept} = 1;
        while ( length $buf < 2 ) {
            $self->read_timeout(
                $self->{mgr_sock}, \$buf, $to_read - length $buf, length $buf,
                $self->{timeout}
            ) or last;
        }
        $self->{stop_keepalive} = ($buf && $buf eq 'OK') ? 0 : time + $self->{keepalive_timeout} + 1;
        warn "stop keepalive_mode until ".$self->{stop_keepalive} if DEBUG && $self->{stop_keepalive} ;
    }
    else {
        $self->cmd_to_mgr("keep",$conn->{md5});
    }
}

sub accept_or_recv {
    my $self = shift;
    my @for_read = @_;
    my $conn;
    for my $pipe_or_sock ( @for_read ) {
        if ( $pipe_or_sock->fileno == $self->{listen_sock}->fileno ) {
            my ($fh,$peer) = $self->{listen_sock}->accept;
            warn sprintf '%s (%d)', $!, $! if ! exists $ok_accept_errno{$!+0};
            next unless $fh;
            $fh->blocking(0);
            setsockopt($fh, IPPROTO_TCP, TCP_NODELAY, 1)
                or die "setsockopt(TCP_NODELAY) failed:$!";
            $conn = {
                fh => $fh,
                fn => $fh->fileno,
                peername => $peer,
                md5 => md5_hex($peer),
                direct => 1,
                reqs => 0,
            };
            last;
        }
        elsif ( $pipe_or_sock->fileno == $self->{lstn_pipe}[READER]->fileno ) {
            my $fd = IO::FDPass::recv(fileno $pipe_or_sock);
            warn sprintf '%s (%d)', $!, $! if ! exists $ok_recv_errno{$!+0};
            if ( $fd >= 0 ) {
                my $fh = IO::Socket::INET->new_from_fd($fd,'r+')
                    or die "unable to convert file descriptor to handle: $!";
                my $peer = $fh->peername;
                next unless $peer;
                $conn = {
                    fh => $fh,
                    fn => $fh->fileno,
                    peername => $peer,
                    md5 => md5_hex($peer),
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
    my $pipelined_buf='';
    my $res = [ 400, [ 'Content-Type' => 'text/plain', 'Connection' => 'close' ], [ 'Bad Request' ] ];

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


1;
