on test => sub {
    requires 'Test::More', 0.98;
};

requires 'Starlet' => '0.18';
requires 'IO::FDPass' => '1.0';
requires 'AnyEvent' => '7.04';
requires 'Plack' => '1.0023';

recommends 'EV' => '4.15';
recommends 'HTTP::Parser::XS' => '0.16';




