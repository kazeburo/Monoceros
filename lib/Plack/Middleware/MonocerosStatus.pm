package Plack::Middleware::MonocerosStatus;

use strict;
use warnings;
use parent qw(Plack::Middleware);
use Plack::Util::Accessor qw(path allow);
use Net::CIDR::Lite;
use IO::Socket::UNIX;

sub prepare_app {
    my $self = shift;
    if ( $self->allow ) {
        my @ip = ref $self->allow ? @{$self->allow} : ($self->allow);
        my @ipv4;
        my @ipv6;
        for (@ip) {
            # hacky check, but actual checks are done in Net::CIDR::Lite.
            if (/:/) {
                push @ipv6, $_;
            } else {
                push @ipv4, $_;
            }
        }
        if ( @ipv4 ) {
            my $cidr4 = Net::CIDR::Lite->new();
            $cidr4->add_any($_) for @ipv4;
            $self->{__cidr4} = $cidr4;
        }
        if ( @ipv6 ) {
            my $cidr6 = Net::CIDR::Lite->new();
            $cidr6->add_any($_) for @ipv6;
            $self->{__cidr6} = $cidr6;
        }
    }
}

sub call {
    my ($self, $env) = @_;
    if( $self->path && $env->{PATH_INFO} eq $self->path ) {
        return $self->_handle_status($env);
    }
    $self->app->($env);
}

sub _handle_status {
    my ($self, $env ) = @_;

    if ( ! $self->allowed($env->{REMOTE_ADDR}) ) {
        return [403, ['Content-Type' => 'text/plain'], [ 'Forbidden' ]];
    }
    
    if ( !$env->{X_MONOCEROS_WORKER_STATS} || ! -f $env->{X_MONOCEROS_WORKER_STATS}) {
        return [500, ['Content-Type' => 'text/plain'], [ 'Monoceros stats file not found' ]];
    }

    open(my $fh, $env->{X_MONOCEROS_WORKER_STATS}) or 
        return [500, ['Content-Type' => 'text/plain'], [ 'Could not open Monoceros stats: $!' ]];
    my $len = $fh->sysread(my $buf, 1024);
    if ( !$len ) {
        return [500, ['Content-Type' => 'text/plain'], [ 'Could not read stats: $!' ]];
    }
    my %stats;
    for my $str ( split /&/, $buf ) {
        my ($key,$val) = split /=/, $str, 2;
        $stats{$key} = $val;
    }
    my $msg = "Total: ".$stats{total}."\015\012";
    $msg .= "Waiting: ".$stats{waiting}."\015\012";
    $msg .= "Processing: ".$stats{processing}."\015\012";
    $msg .= "MaxWorkers: ".$stats{max_workers}."\015\012\015\012";
    
    return [200, ['Content-Type' => 'text/plain'], [$msg]];
}

sub allowed {
    my ( $self , $address ) = @_;
    if ( $address =~ /:/) {
        return unless $self->{__cidr6};
        return $self->{__cidr6}->find( $address );
    }
    return unless $self->{__cidr4};
    return $self->{__cidr4}->find( $address );
}


1;

__END__

=head1 NAME

Plack::Middleware::MonocerosStatus - show Monoceros connection manager status

=head1 SYNOPSIS

  use Plack::Builder;

  builder {
      enable "MonocerosStatus",
          path => '/monoceros-status',
          allow => [ '127.0.0.1', '192.168.0.0/16' ],
      $app;
  };

  % curl http://server:port/monoceros-status
  Total: 100
  Waiting: 98
  Processing: 2

=head1 DESCRIPTION

Plack::Middleware::MonocerosStatus is a middleware to display Monoceros manager status.

  Waiting - a number of keepalive socket, waiting to next request
  Processing - a number of request arrived socket, sockets are queued or processed by worker
  Total - Waiting + Processing

=head1 CONFIGURATIONS

=over 4

=item path

  path => '/server-status',

location that displays server status

=item allow

  allow => '127.0.0.1'
  allow => ['192.168.0.0/16', '10.0.0.0/8']

host based access control of a page of server status. supports IPv6 address.

=back

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo {at} gmail.comE<gt>

=head1 SEE ALSO

L<Monoceros>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
