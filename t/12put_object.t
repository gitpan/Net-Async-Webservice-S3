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
   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

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

# Multipart put from part generator
{
   my @parts = map {
      my $str = $_; [ length $str, sub { substr( $str, $_[0], $_[1] ) } ]
   } "The first part", "The second part";

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "two",
      gen_parts => sub { @parts ? @{ shift @parts } : () },
   );

   my $req;
   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

   is( $req->method,         "POST",                    'Initiate request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Initiate request URI authority' );
   is( $req->uri->path,      "/two",                    'Initiate request URI path' );
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
  <Key>two</Key>
  <UploadId>ABCDEFG</UploadId>
</InitiateMultipartUploadResult>
EOF
   );

   # Now wait on the chunks
   foreach ( [ 1, "The first part" ], [ 2, "The second part" ] ) {
      my ( $part_num, $content ) = @$_;

      wait_for { $req = $http->pending_request or $f->is_ready };
      $f->get if $f->is_ready and $f->failure;

      is( $req->method,     "PUT",    'Part request method' );
      is( $req->uri->path,  "/two",   'Part request URI path' );
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

   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

   is( $req->method,         "POST",                    'Complete request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Complete request URI authority' );
   is( $req->uri->path,      "/two",                    'Complete request URI path' );
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
  <Key>two</Key>
  <ETag>"3858f62230ac3c915f300c664312c11f-2"</ETag>
</CompleteMultipartUploadResult>
EOF
   );

   wait_for { $f->is_ready };

   my ( $etag ) = $f->get;
   is( $etag, '"3858f62230ac3c915f300c664312c11f-2"', 'result of multipart put' );
}

# Multipart put from Future
{
   my $content = "Content that comes later";

   my @parts = (
      [ length $content, my $value_f = Future->new ],
   );

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one",
      gen_parts => sub { @parts ? @{ shift @parts } : () },
   );

   my $req;
   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

   is( $req->method,         "POST",                    'Initiate request method' );
   is( $req->uri->query,     "uploads",                 'Initiate request URI query' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "application/xml",
      ], <<'EOF' )
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>bucket</Bucket>
  <Key>one</Key>
  <UploadId>ABCDEFG</UploadId>
</InitiateMultipartUploadResult>
EOF
   );

   $value_f->done( $content );

   foreach ( [ 1, $content ] ) {
      my ( $part_num, $content ) = @$_;

      wait_for { $req = $http->pending_request or $f->is_ready };
      $f->get if $f->is_ready and $f->failure;

      is( $req->method,     "PUT",    'Part request method' );
      is( $req->uri->path,  "/one",   'Part request URI path' );
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

   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

   is( $req->method,         "POST",                    'Complete request method' );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", 'Complete request URI authority' );
   is( $req->uri->path,      "/one",                    'Complete request URI path' );
   is( $req->uri->query,     "uploadId=ABCDEFG",        'Complete request URI query' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "application/xml",
      ], <<'EOF' )
<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://bucket.s3.amazonaws.com/three</Location>
  <Bucket>bucket</Bucket>
  <Key>two</Key>
  <ETag>"3858f62230ac3c915f300c664312c11f-2"</ETag>
</CompleteMultipartUploadResult>
EOF
   );

   wait_for { $f->is_ready };

   my ( $etag ) = $f->get;
   is( $etag, '"3858f62230ac3c915f300c664312c11f-2"', 'result of multipart put from Future' );
}

# Multipart put from value ((deprecated))
{
   $s3->configure( part_size => 16 );

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "three",
      value  => "Content too long for one chunk",
   );

   my $req;
   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

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

   foreach ( [ 1, "Content too long" ], [ 2, " for one chunk" ] ) {
      my ( $part_num, $content ) = @$_;

      wait_for { $req = $http->pending_request or $f->is_ready };
      $f->get if $f->is_ready and $f->failure;

      my $md5 = md5_hex( $content );

      $http->respond(
         HTTP::Response->new( 200, "OK", [
            ETag => qq("$md5"),
         ], "" )
      );
   }

   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

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
   is( $etag, '"3858f62230ac3c915f300c664312c11f-2"', 'result of multipart put from value' );

   # Restore it
   $s3->configure( part_size => 100*1024*1024 );
}

# Streaming too short
{
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one.short",
      gen_value => do { my $once; sub {
         $once++ ? undef : "Too short";
      }},
      value_length => 30,
   );

   my $req;
   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

   is( $req->content, "Too short" . "\0"x21, 'Request body null-padded' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         ETag => '"2cff329fda964729d89d8ce24d3277b6"',
      ], "" )
   );

   wait_for { $f->is_ready };
   $f->get;
}

# Streaming too long
{
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one.long",
      gen_value => do { my $once; sub {
         $once++ ? undef : "This value will be too long and get truncated";
      }},
      value_length => 20,
   );

   my $req;
   wait_for { $req = $http->pending_request or $f->is_ready };
   $f->get if $f->is_ready and $f->failure;

   is( $req->content, "This value will be t", 'Request body truncated' );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         ETag => '"cab29dbf12ac7eb51234ab3ceb3631d5"',
      ], "" )
   );

   wait_for { $f->is_ready };
   $f->get;
}

done_testing;
