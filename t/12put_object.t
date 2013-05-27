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

# Simple put
{
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one",
      value  => "a new value",
   );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "PUT",                     'Request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Request URI authority' );
   is( $req->uri->path,      "/one",                    'Request URI path' );
   is( $req->content,        "a new value",             'Request body' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         ETag => '"895feaa3ad7b47130e2314bd88cab3b0"',
      ], "" )
   );

   wait_for { $f->is_ready };

   my ( $md5sum ) = $f->get;
   is( $md5sum, "895feaa3ad7b47130e2314bd88cab3b0", 'result of simple put' );
}

# Streaming put
{
   my $content = "another value for the key";

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one",
      gen_value => sub {
         return undef unless length $content;
         return substr( $content, 0, 4, "" );
      },
      value_length => length $content,
   );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "PUT",                     'Request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Request URI authority' );
   is( $req->uri->path,      "/one",                    'Request URI path' );
   is( $req->content,        "another value for the key", 'Request body' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         ETag => '"b9f0d1cf2483ebab3e717e122df9a5c6"',
      ], "" )
   );

   wait_for { $f->is_ready };

   my ( $md5sum ) = $f->get;
   is( $md5sum, "b9f0d1cf2483ebab3e717e122df9a5c6", 'result of streaming put' );
}

done_testing;
