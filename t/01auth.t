#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use t::TestHTTP;

use IO::Async::Test;
use IO::Async::Loop;

use HTTP::Response;

my $time;
BEGIN {
   # Auth hash includes signature of the date. So we need them repeatable for
   # this test
   *CORE::GLOBAL::time = sub { return $time };
}

use Net::Async::Webservice::S3;

my $loop = IO::Async::Loop->new;
testing_loop( $loop );

my $s3 = Net::Async::Webservice::S3->new(
   http => my $http = TestHTTP->new,
   access_key => 'ABCDEFGHIJKLMNOPQRST',
   secret_key => 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLOP',
);

$loop->add( $s3 );

ok( defined $s3, 'defined $s3' );
isa_ok( $s3, "Net::Async::Webservice::S3", '$s3' );

# list_bucket
{
   $time = 1369587977; # when I happened to write this test

   my $f = $s3->list_bucket(
      bucket => "bucket",
      prefix => "",
      delimiter => "/",
   );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method, "GET", '$req->method' );
   is( $req->uri, "http://bucket.s3.amazonaws.com/?delimiter=%2F&max-keys=100&prefix=", '$req->uri' );

   # Assert the date header, as auth depends on it
   is( $req->header( "Date" ), "Sun, 26 May 2013 17:06:17 GMT", '$req->header("Date")' );

   is( $req->authorization,
       "AWS ABCDEFGHIJKLMNOPQRST:L0A3s2Ks+IdDGuW9NNad5iIsKn4=",
       '$req->authorization' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "application/xml",
      ], <<'EOF' )
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix></Prefix>
  <Marker></Marker>
  <MaxKeys>100</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>
EOF
   );

   wait_for { $f->is_ready };

   is( scalar $f->failure, undef, '$f is done with no failure' );
}

done_testing;
