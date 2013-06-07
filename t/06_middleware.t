use strict;
use warnings;

use Plack::Handler::Monoceros;
use Plack::Loader;
use Plack::Builder;
use LWP::UserAgent;
use Test::TCP;
use Test::More;


my $app = builder {
    enable "MonocerosStatus",
        path => '/monoceros-status',
        allow => ['0.0.0.0/0','::/0'];
    sub {
        [ 200, [ 'Content-Type' => 'text/plain' ], [ "hello world" ] ]
    };
};

test_tcp(
    client => sub {
        my ($port, $server_pid) = @_;
        my $ua = LWP::UserAgent->new();
        my $res = $ua->get(sprintf('http://localhost:%s/monoceros-status',$port));
        like $res->content, qr/Total: \d+/;
        like $res->content, qr/Waiting: \d+/;
        like $res->content, qr/Processing: \d+/;
    },
    server => sub {
        my $port = shift;
        Plack::Loader->load('Monoceros',port => $port )->run($app);
        exit;
    },
);

done_testing;
