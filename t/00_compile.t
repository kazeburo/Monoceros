use strict;
use Test::More;

use_ok $_ for qw(
    Monoceros
    Monoceros::Server
    Plack::Handler::Monoceros
    Plack::Middleware::MonocerosStatus
);

done_testing;

