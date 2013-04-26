use strict;
use warnings;

use Plack::Handler::Monoceros;
use Plack::Loader;
use LWP::UserAgent;
use Test::TCP;
use Test::More;


test_tcp(
    client => sub {
        my ($port, $server_pid) = @_;
        unless (my $pid = fork) {
            die "fork failed:$!"
                unless defined $pid;
            # child process
            sleep 1;
            kill 'TERM', $server_pid;
            exit 0;
        }
        my $ua = LWP::UserAgent->new();
        my $res = $ua->get(sprintf('http://localhost:%s/',$port));
        is $res->content, 'hello world';
    },
    server => sub {
        my $port = shift;
        Plack::Loader->load('Monoceros',port => $port )->run(
            sub{
                sleep 5;
                [ 200, [ 'Content-Type' => 'text/plain' ], [ "hello world" ] ]
            }
        );
        exit;
    },
);

done_testing;
