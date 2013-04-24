use strict;
use Test::More;
use Plack::Test::Suite;
use Plack::Handler::Monoceros;

Plack::Test::Suite->run_server_tests('Monoceros');
done_testing();
