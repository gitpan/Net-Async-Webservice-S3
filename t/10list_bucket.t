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

# Simple list
{
   my $f = $s3->list_bucket(
      bucket => "bucket",
      prefix => "",
      delimiter => "/",
   );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "GET",                     'Request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Request URI authority' );
   is( $req->uri->path,      "/",                       'Request URI path' );
   is_deeply( [ $req->uri->query_form ],
              [ delimiter  => "/",
                'max-keys' => 100,
                prefix     => "" ],
              'Request URI query parameters' );

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

  <Contents>
    <Key>one</Key>
    <LastModified>2013-05-27T00:58:50.000Z</LastModified>
    <ETag>&quot;5f4af733fd99d974d92cd7e8d4efdf9f&quot;</ETag>
    <Size>16</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>

  <CommonPrefixes>
    <Prefix>foo/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>bar/</Prefix>
  </CommonPrefixes>
</ListBucketResult>
EOF
   );

   wait_for { $f->is_ready };

   my ( $keys, $prefixes ) = $f->get;

   is_deeply( $keys,
              [ {
                    key => "one",
                    last_modified => "2013-05-27T00:58:50.000Z",
                    etag => '"5f4af733fd99d974d92cd7e8d4efdf9f"',
                    size => 16,
                    storage_class => "STANDARD",
                 } ],
              'list_bucket keys' );

   is_deeply( $prefixes,
              [ qw(
                  foo/
                  bar/
                ) ],
              'list_bucket prefixes' );
}

# List continuation
{
   my $f = $s3->list_bucket(
      bucket => "bucket",
      prefix => "",
      delimiter => "/",
   );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "GET",                     'Request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Request URI authority' );
   is( $req->uri->path,      "/",                       'Request URI path' );
   is_deeply( [ $req->uri->query_form ],
              [ delimiter  => "/",
                'max-keys' => 100,
                prefix     => "" ],
              'Request URI query parameters' );

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
  <IsTruncated>true</IsTruncated>

  <Contents>
    <Key>one</Key>
    <LastModified>2013-05-27T00:58:50.000Z</LastModified>
    <ETag>&quot;5f4af733fd99d974d92cd7e8d4efdf9f&quot;</ETag>
    <Size>16</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>
EOF
   );

   wait_for { $req = $http->pending_request };

   is( $req->method,         "GET",                     'Request part2 method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Request part2 URI authority' );
   is( $req->uri->path,      "/",                       'Request part2 URI path' );
   is_deeply( [ $req->uri->query_form ],
              [ delimiter  => "/",
                marker     => "one",
                'max-keys' => 100,
                prefix     => "" ],
              'Request part2 URI query parameters' );

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

  <Contents>
    <Key>two</Key>
    <LastModified>2013-05-27T02:40:38.000Z</LastModified>
    <ETag>&quot;5f4af733fd99d974d92cd7e8d4efdf9f&quot;</ETag>
    <Size>16</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>
EOF
   );

   wait_for { $f->is_ready };

   my ( $keys ) = $f->get;

   is_deeply( $keys,
              [ {
                    key => "one",
                    last_modified => "2013-05-27T00:58:50.000Z",
                    etag => '"5f4af733fd99d974d92cd7e8d4efdf9f"',
                    size => 16,
                    storage_class => "STANDARD",
                 },
                {
                    key => "two",
                    last_modified => "2013-05-27T02:40:38.000Z",
                    etag => '"5f4af733fd99d974d92cd7e8d4efdf9f"',
                    size => 16,
                    storage_class => "STANDARD",
                 } ],
              'list_bucket keys from continued response' );
}

done_testing;
