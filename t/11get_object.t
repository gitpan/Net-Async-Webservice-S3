#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use t::TestHTTP;

use IO::Async::Test;
use IO::Async::Loop;

use HTTP::Response;

use Net::Async::Webservice::S3;

my $loop = IO::Async::Loop->new;
testing_loop( $loop );

my $s3 = Net::Async::Webservice::S3->new(
   http => my $http = TestHTTP->new,
   access_key => 'K'x20,
   secret_key => 's'x40,
);

$loop->add( $s3 );

# Simple get
{
   my $f = $s3->get_object(
      bucket => "bucket",
      key    => "one",
   );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "GET",                     'Request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Request URI authority' );
   is( $req->uri->path,      "/one",                    'Request URI path' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "text/plain",
      ], <<'EOF' )
Here is the key
EOF
   );

   wait_for { $f->is_ready };

   my ( $value, $response ) = $f->get;
   is( $value, "Here is the key\n", '$value for simple get' );
   is( $response->content_type, "text/plain", '$response->content_type for simple get' );
}

# Streaming get
{
   my ( $header, $bytes );
   my $f = $s3->get_object(
      bucket => "bucket",
      key    => "one",
      on_chunk => sub {
         ( $header, $bytes ) = @_;
      },
   );

   wait_for { $http->pending_request };
   $http->respond_header(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "text/plain",
      ], "" )
   );

   $http->respond_more( "some bytes" );

   wait_for { defined $header };
   is( $header->content_type, "text/plain", '$header->content_type for chunked get' );
   is( $bytes, "some bytes", '$bytes for chunked get' );

   $http->respond_done;

   wait_for { $f->is_ready };
}

done_testing;
