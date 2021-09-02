use strict;
use warnings;

use LWP::UserAgent;
use Plack::Runner;
use Test::More;
use Test::TCP;

my %seen_remote_pids;
my $number_of_requests = 6;
test_tcp(
    server => sub {
        my $port = shift;
        my $runner = Plack::Runner->new;
        $runner->parse_options(
            qw(--server Monoceros --max-workers 2  --max_reqs_per_child 1 --port), $port,
        );
        $runner->run(
            sub {
                my $env = shift;
                return [
                    200,
                    [ 'Content-Type' => 'text/plain' ],
                    [ $env->{X_REMOTE_PID} ],
                ];
            },
        );
        exit;
    },
    client => sub {
        my $port = shift;
        my $ua = LWP::UserAgent->new;
        $ua->timeout(10);
        foreach my $i ((1..$number_of_requests)) {
        note "send request $i";
            my $res = $ua->get("http://127.0.0.1:$port/");
            ok $res->is_success;
            is $res->code, 200;
            $seen_remote_pids{$res->content}++;
        }
    },
);
is(scalar keys %seen_remote_pids, $number_of_requests);

done_testing;
