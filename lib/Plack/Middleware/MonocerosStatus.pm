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
    
    if ( !$env->{X_MONOCEROS_WORKER_SOCK} ) {
        return [500, ['Content-Type' => 'text/plain'], [ 'Monoceros sock file not found' ]];
    }

    my $fh = IO::Socket::UNIX->new(
        Type => SOCK_STREAM,
        Peer => $env->{X_MONOCEROS_WORKER_SOCK}
    ) or return [500, ['Content-Type' => 'text/plain'], [ 'Could not open Monoceros sock: $!' ]];
    $fh->syswrite('stat '.'x'x32);
    my $len = $fh->sysread(my $buf, 1024);
    if ( !$len ) {
        return [500, ['Content-Type' => 'text/plain'], [ 'Could not read status: $!' ]];
    }
    return [200, ['Content-Type' => 'text/plain'], [$buf]];
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
      enable "Plack::Middleware::ServerStatus::Lite",
          path => '/monoceros-status',
          allow => [ '127.0.0.1', '192.168.0.0/16' ],
      $app;
  };

  % curl http://server:port/monoceros-status
  Processing: 2
  Waiting: 98
  Queued: 0

=head1 DESCRIPTION

Plack::Middleware::MonocerosStatus is a middleware to display Monoceros manager status.

  Processing - a number of socket processed by worker
  Waiting - a number of keepalive socket, waiting to next request
  Queued - a number of request arrived socket, waiting to process by worker

=head1 CONFIGURATIONS

=over 4

=item path

  path => '/server-status',

location that displays server status

=item allow

  allow => '127.0.0.1'
  allow => ['192.168.0.0/16', '10.0.0.0/8']

host based access control of a page of server status. supports IPv6 address.

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo {at} gmail.comE<gt>

=head1 SEE ALSO

L<Monoceros>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
