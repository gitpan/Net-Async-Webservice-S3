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

   my ( $etag ) = $f->get;
   is( $etag, '"895feaa3ad7b47130e2314bd88cab3b0"', 'result of simple put' );
}

# Streaming put
{
   my $content = "another value for the key";
   my @gen_value_args;

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one",
      gen_value => sub {
         push @gen_value_args, [ $_[0], $_[1] ];
         return substr( $content, $_[0], $_[1] );
      },
      value_length => length $content,
   );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "PUT",                     'Request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Request URI authority' );
   is( $req->uri->path,      "/one",                    'Request URI path' );
   is( $req->content,        "another value for the key", 'Request body' );

   is_deeply( \@gen_value_args,
              [ [ 0, 25 ] ],
              'args to gen_value callback' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         ETag => '"b9f0d1cf2483ebab3e717e122df9a5c6"',
      ], "" )
   );

   wait_for { $f->is_ready };

   my ( $etag ) = $f->get;
   is( $etag, '"b9f0d1cf2483ebab3e717e122df9a5c6"', 'result of streaming put' );
}

# Multipart put
{
   $s3->configure( multipart_chunk_size => 16 );

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "three",
      value  => "Content too long for one chunk",
   );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "POST",                    'Initiate request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Initiate request URI authority' );
   is( $req->uri->path,      "/three",                  'Initiate request URI path' );
   is( $req->uri->query,     "uploads",                 'Initiate request URI query' );

   # Technically this isn't a valid S3 UploadId but nothing checks the
   # formatting so it's probably OK
   $http->respond(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "application/xml",
      ], <<'EOF' )
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>bucket</Bucket>
  <Key>three</Key>
  <UploadId>ABCDEFG</UploadId>
</InitiateMultipartUploadResult>
EOF
   );

   # Now wait on the chunks
   foreach ( [ 1, "Content too long" ], [ 2, " for one chunk" ] ) {
      my ( $part_num, $content ) = @$_;

      wait_for { $req = $http->pending_request };

      is( $req->method,     "PUT",    'Part request method' );
      is( $req->uri->path,  "/three", 'Part request URI path' );
      is( $req->uri->query, "partNumber=$part_num&uploadId=ABCDEFG",
         'Part request URI query' );
      is( $req->content,    $content, 'Part request body' );

      my $md5 = md5_hex( $content );

      $http->respond(
         HTTP::Response->new( 200, "OK", [
            ETag => qq("$md5"),
         ], "" )
      );
   }

   wait_for { $req = $http->pending_request };

   is( $req->method,         "POST",                    'Complete request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Complete request URI authority' );
   is( $req->uri->path,      "/three",                  'Complete request URI path' );
   is( $req->uri->query,     "uploadId=ABCDEFG",        'Complete request URI query' );

   is( $req->content_length, length( $req->content ), 'Complete request has Content-Length' );
   like( $req->header( "Content-MD5" ), qr/^[0-9A-Z+\/]{22}==$/i,
      'Complete request has valid Base64-encoded MD5 sum' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "application/xml",
      ], <<'EOF' )
<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://bucket.s3.amazonaws.com/three</Location>
  <Bucket>bucket</Bucket>
  <Key>three</Key>
  <ETag>"3858f62230ac3c915f300c664312c11f-2"</ETag>
</CompleteMultipartUploadResult>
EOF
   );

   wait_for { $f->is_ready };

   my ( $etag ) = $f->get;
   is( $etag, '"3858f62230ac3c915f300c664312c11f-2"', 'result of multipart put' );
}

done_testing;
