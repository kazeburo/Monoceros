use strict;
use Test::More;
use Plack::Handler::Monoceros;
use Plack::Test::Suite;


Plack::Test::Suite->run_server_tests('Monoceros');
done_testing();
