use strict;
use Test::More;
use Plack::Test::Suite;
use Plack::Handler::Starcross;

Plack::Test::Suite->run_server_tests('Starcross');
done_testing();
