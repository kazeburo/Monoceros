# NAME

Monoceros - PSGI/Plack server with event driven connection manager, preforking workers

# SYNOPSIS

    % plackup -s Monoceros --max-keepalive-reqs=10000 --max-workers=2 -a app.psgi

# DESCRIPTION

Monoceros is PSGI/Plack server supports HTTP/1.0. Monoceros has a event-driven 
connection manager and preforking workers. Monoceros can keep large amount of 
connection at minimal processes.

                                                          +--------+
                                                      +---+ worker |
          TCP       +---------+   UNIX DOMAIN SOCKET  |   +--------+
    --------------- | manager | ----------------------+ 
                    +---------+                       |   +--------+
    <- keepalive ->              <-- passing fds -->  `---+ worker |
                                                          +--------+

And this server inherit [Starlet](http://search.cpan.org/perldoc?Starlet). Monoceros supports following features too.

\- prefork and graceful shutdown using [Parallel::Prefork](http://search.cpan.org/perldoc?Parallel::Prefork)

\- hot deploy using [Server::Starter](http://search.cpan.org/perldoc?Server::Starter)

\- fast HTTP processing using [HTTP::Parser::XS](http://search.cpan.org/perldoc?HTTP::Parser::XS) (optional)

But Monoceros does not support spawn-interval.

# COMMAND LINE OPTIONS

In addition to the options supported by [plackup](http://search.cpan.org/perldoc?plackup), Monoceros accepts following options(s).

## \--max-workers=\#

number of worker processes (default: 10)

## \--timeout=\#

seconds until timeout (default: 300)

## \--keepalive-timeout=\#

timeout for persistent connections (default: 2)

## \--max-keepalive-reqs=\#

max. number of requests allowed per single persistent connection.  If set to one, persistent connections are disabled (default: 1)

## \--max-reqs-per-child=\#

max. number of requests to be handled before a worker process exits (default: 100)

## \--min-reqs-per-child=\#

if set, randomizes the number of requests handled by a single worker process between the value and that supplied by `--max-reqs-per-chlid` (default: none)

# SEE ALSO

[Starlet](http://search.cpan.org/perldoc?Starlet), [Server::Starter](http://search.cpan.org/perldoc?Server::Starter), [AnyEvent](http://search.cpan.org/perldoc?AnyEvent)

# LICENSE      

Copyright (C) Masahiro Nagano

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Masahiro Nagano <kazeburo@gmail.com>
