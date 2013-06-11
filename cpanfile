on test => sub {
    requires 'Test::More', '0.98';
    requires 'Test::TCP', '1.30';
    requires 'LWP::UserAgent';
    requires 'Plack::Test::Suite';
};

requires 'Starlet' => '0.18';
requires 'IO::FDPass' => '1.0';
requires 'AnyEvent' => '7.04';
requires 'Plack' => '1.0023';
requires 'Net::CIDR::Lite';
requires 'POSIX::getpeername';
requires 'POSIX::Socket';

suggests 'EV' => '4.15';
suggests 'HTTP::Parser::XS' => '0.16';
suggests 'Guard' => '1.022';






