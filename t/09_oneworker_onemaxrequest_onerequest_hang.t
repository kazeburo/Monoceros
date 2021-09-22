use strict;
use warnings;

use LWP::UserAgent;
use Plack::Runner;
use Test::More;
use Test::TCP;
use List::Util 'sum';

my %seen_remote_pids;
my $number_of_requests = 1;
test_tcp(
    server => sub {
        my $port = shift;
        my $runner = Plack::Runner->new;
        $runner->parse_options(
            qw(--server Monoceros --max-workers 1 --max_reqs_per_child 3 --keepalive 1 --port), $port,
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
            ok $res->is_success, 'max_workers 1, max_reqs 3, keepalive 1';
            is $res->code, 200;
            $seen_remote_pids{$res->content}++;
        }
    },
);
is(scalar keys %seen_remote_pids, $number_of_requests);
%seen_remote_pids = ();

test_tcp(
    server => sub {
        my $port = shift;
        my $runner = Plack::Runner->new;
        $runner->parse_options(
            qw(--server Monoceros --max-workers 1 --max_reqs_per_child 2 --keepalive 0 --port), $port,
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
            ok $res->is_success, 'max_workers 1, max_reqs 2, keepalive 0';
            is $res->code, 200;
            $seen_remote_pids{$res->content}++;
        }
    },
);
is(scalar keys %seen_remote_pids, $number_of_requests);
%seen_remote_pids = ();

test_tcp(
    server => sub {
        my $port = shift;
        my $runner = Plack::Runner->new;
        $runner->parse_options(
            qw(--server Monoceros --max-workers 1 --max_reqs_per_child 2 --keepalive 1 --port), $port,
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
            ok $res->is_success, 'max_workers 1, max_reqs 2, keepalive 1';
            is $res->code, 200;
            $seen_remote_pids{$res->content}++;
        }
    },
);
is(scalar keys %seen_remote_pids, $number_of_requests);
%seen_remote_pids = ();

$number_of_requests = 3;
test_tcp(
    server => sub {
        my $port = shift;
        my $runner = Plack::Runner->new;
        $runner->parse_options(
            qw(--server Monoceros --max-workers 1 --max_reqs_per_child 2 --keepalive 1 --port), $port,
        );
        $runner->run(
            sub {
                #local $SIG{TERM} = sub { print STDERR "TERM called" };
                #local $SIG{USR1} = sub { print STDERR "USR1 called" };
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
            ok $res->is_success, 'max_workers 1, max_reqs 2, keepalive 1';
            is $res->code, 200;
            $seen_remote_pids{$res->content}++;
        }
    },
);
is(scalar keys %seen_remote_pids, $number_of_requests-1, 'Number of distinct worker PIDs as expected');
is(sum(values %seen_remote_pids), $number_of_requests, 'Number of request responses as expected');
%seen_remote_pids = ();


done_testing;
