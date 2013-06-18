#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use t::TestHTTP;

use IO::Async::Test;
use IO::Async::Loop;
use Digest::MD5 qw( md5_hex );

use HTTP::Response;

use Net::Async::Webservice::S3;

my $loop = IO::Async::Loop->new;
testing_loop( $loop );

my $s3 = Net::Async::Webservice::S3->new(
   http => my $http = TestHTTP->new,
   access_key => 'K'x20,
   secret_key => 's'x40,

   max_retries => 1,
);

$loop->add( $s3 );

sub await_upload_and_respond
{
   my ( $key, $content ) = @_;

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "PUT",                     "Request method for $key" );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", "Request URI authority for $key" );
   is( $req->uri->path,      "/$key",                   "Request URI path for $key" );
   is( $req->content,        $content,                  "Request body for $key" );

   my $md5 = md5_hex( $req->content );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         ETag => qq("$md5"),
      ], "" )
   );

   return ( $req );
}

# Single PUT from string
{
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one",
      value  => "a new value",
   );
   $f->on_fail( sub { die @_ } );

   await_upload_and_respond "one", "a new value";

   wait_for { $f->is_ready };

   my ( $etag ) = $f->get;
   is( $etag, '"895feaa3ad7b47130e2314bd88cab3b0"', 'result of simple put' );
}

# Single PUT from Future
{
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "two",
      value  => my $value_f = Future->new,
   );
   $f->on_fail( sub { die @_ } );

   $loop->later( sub { $value_f->done( "Deferred content that came later" ) } );

   await_upload_and_respond "two", "Deferred content that came later";

   $f->get;
}

# Single PUT from CODE/size pair
{
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "three",
      value  => sub { substr( "Content from a CODE ref", $_[0], $_[1] ) },
      value_length => 23,
   );
   $f->on_fail( sub { die @_ } );

   await_upload_and_respond "three", "Content from a CODE ref";

   $f->get;
}

# Single PUT with extra metadata
{
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "ONE",
      value  => "the value",
      meta   => {
         A      => "a",
      },
   );
   $f->on_fail( sub { die @_ } );

   my ( $request ) = await_upload_and_respond "ONE", "the value";
   is( $request->header( "X-Amz-Meta-A" ), "a", '$request has X-Amz-Meta-A header' );

   $f->get;
}

done_testing;
