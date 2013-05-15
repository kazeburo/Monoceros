package Monoceros;

use strict;
use warnings;
use 5.008005;

our $VERSION = "0.09";

1;
__END__

=encoding utf-8

=head1 NAME

Monoceros - PSGI/Plack server with event driven connection manager, preforking workers

=head1 SYNOPSIS

    % plackup -s Monoceros --max-keepalive-reqs=10000 --max-workers=2 -a app.psgi

=head1 DESCRIPTION

Monoceros is PSGI/Plack server supports HTTP/1.1. Monoceros has a event-driven 
connection manager and preforking workers. Monoceros can keep large amount of 
connection at minimal processes.

                                                          +--------+
                                                      +---+ worker |
          TCP       +---------+   UNIX DOMAIN SOCKET  |   +--------+
    --------------- | manager | ----------------------+ 
                    +---------+                       |   +--------+
    <- keepalive ->              <-- passing fds -->  `---+ worker |
                                                          +--------+

Features of Monoceros

- a manager process based on L<AnyEvent> keeps over C10K connections

- uses L<IO::FDPass> for passing a file descriptor to workers

- supports HTTP/1.1 and also supports HTTP/1.0 keepalive

And this server inherit L<Starlet>. Monoceros supports following features too.

- prefork and graceful shutdown using L<Parallel::Prefork>

- hot deploy using L<Server::Starter>

- fast HTTP processing using L<HTTP::Parser::XS> (optional)

But Monoceros does not support max-keepalive-reqs and spawn-interval.

=head1 COMMAND LINE OPTIONS

In addition to the options supported by L<plackup>, Monoceros accepts following options(s).
Note, the default value of several options is different from Starlet.

=head2 --max-workers=#

number of worker processes (default: 5)

=head2 --timeout=#

seconds until timeout (default: 300)

=head2 --keepalive-timeout=#

timeout for persistent connections (default: 10)

=head2 --max-reqs-per-child=#

max. number of requests to be handled before a worker process exits (default: 100)

=head2 --min-reqs-per-child=#

if set, randomizes the number of requests handled by a single worker process between the value and that supplied by C<--max-reqs-per-chlid> (default: none)

=head1 RECOMMENDED MODULES

For more performance. I recommends you to install these module.

- L<EV>

- L<HTTP::Parser::XS>

=head1 SEE ALSO

L<Starlet>, L<Server::Starter>, L<AnyEvent>, L<IO::FDPass>

=head1 LICENSE      

Copyright (C) Masahiro Nagano

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo@gmail.comE<gt>

