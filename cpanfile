on test => sub {
    requires 'Test::More', '0.98';
    requires 'Test::TCP', '1.26';
    requires 'LWP::UserAgent';
    requires 'Plack::Test::Suite';
};

requires 'Starlet' => '0.18';
requires 'IO::FDPass' => '1.0';
requires 'AnyEvent' => '7.04';
requires 'Plack' => '1.0023';

suggests 'EV' => '4.15';
suggests 'HTTP::Parser::XS' => '0.16';





