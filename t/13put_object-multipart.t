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
   max_retries => 1,
   http => my $http = TestHTTP->new,
   access_key => 'K'x20,
   secret_key => 's'x40,
);

$loop->add( $s3 );

sub await_multipart_initiate_and_respond
{
   my ( $key ) = @_;

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "POST",                    "Initiate request method for $key" );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", "Initiate request URI authority for $key" );
   is( $req->uri->path,      "/$key",                   "Initiate request URI path for $key" );
   is( $req->uri->query,     "uploads",                 "Initiate request URI query for $key" );

   # Technically this isn't a valid S3 UploadId but nothing checks the
   # formatting so it's probably OK
   $http->respond(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "application/xml",
      ], <<"EOF" )
<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>bucket</Bucket>
  <Key>$key</Key>
  <UploadId>ABCDEFG</UploadId>
</InitiateMultipartUploadResult>
EOF
   );

   return ( $req );
}

sub await_multipart_part_and_respond
{
   my ( $key, $part_num, $content ) = @_;

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,     "PUT",    "Part request method for $key" );
   is( $req->uri->path,  "/$key",  "Part request URI path for $key" );
   is( $req->uri->query, "partNumber=$part_num&uploadId=ABCDEFG",
      "Part request URI query for $key" );
   is( $req->content,    $content, "Part request body for $key" );

   my $md5 = md5_hex( $req->content );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
            ETag => qq("$md5"),
         ], "" )
   );

   return ( $req );
}

sub await_multipart_complete_and_respond
{
   my ( $key ) = @_;

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method,         "POST",                    "Complete request method for $key" );
   is( $req->uri->authority, "bucket.s3.amazonaws.com", "Complete request URI authority for $key" );
   is( $req->uri->path,      "/$key",                   "Complete request URI path for $key" );
   is( $req->uri->query,     "uploadId=ABCDEFG",        "Complete request URI query for $key" );

   is( $req->content_length, length( $req->content ), "Complete request has Content-Length" );
   like( $req->header( "Content-MD5" ), qr/^[0-9A-Z+\/]{22}==$/i,
      "Complete request has valid Base64-encoded MD5 sum" );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
         Content_Type => "application/xml",
      ], <<"EOF" )
<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://bucket.s3.amazonaws.com/three</Location>
  <Bucket>bucket</Bucket>
  <Key>$key</Key>
  <ETag>"3858f62230ac3c915f300c664312c11f-2"</ETag>
</CompleteMultipartUploadResult>
EOF
   );

   return ( $req );
}

# Multipart put from strings
{
   my @parts = ( "The first part", "The second part" );

   my @written;
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "four",
      gen_parts => sub { return unless @parts; shift @parts },
      on_write => sub { push @written, $_[0] },
   );
   $f->on_fail( sub { die @_ } );

   await_multipart_initiate_and_respond "four";

   # Now wait on the chunks
   foreach ( [ 1, "The first part" ], [ 2, "The second part" ] ) {
      await_multipart_part_and_respond "four", @$_;
   }

   await_multipart_complete_and_respond "four";

   wait_for { $f->is_ready };

   my ( $etag, $len ) = $f->get;
   is( $etag, '"3858f62230ac3c915f300c664312c11f-2"', 'result of multipart put' );
   is( $len, 29, '$length of multipart put' );

   is_deeply( \@written,
              [ 14, 14+15 ], 'on_write invoked on each chunk with total' );
}

# Multipart put from Future
{
   my @parts = (
      my $value1_f = Future->new,
      my $value2_f = Future->new,
   );

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "five",
      gen_parts => sub { return unless @parts; shift @parts },
   );
   $f->on_fail( sub { die @_ } );

   $loop->later( sub { $value1_f->done( "The content that " ) } );
   $loop->later( sub { $value2_f->done( "comes later" ) } );

   await_multipart_initiate_and_respond "five";

   foreach ( [ 1, "The content that " ], [ 2, "comes later" ] ) {
      await_multipart_part_and_respond "five", @$_;
   }

   await_multipart_complete_and_respond "five";

   wait_for { $f->is_ready };

   my ( $etag, $len ) = $f->get;
   is( $len, 28, '$length from multipart put from Future' );
}

# Multipart put from CODE/size pairs
{
   my @parts = (
      [ sub { substr( "Content generated ", $_[0], $_[1] ) }, 18 ],
      [ sub { substr( "by code", $_[0], $_[1] ) }, 7 ],
   );

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "six",
      gen_parts => sub { return unless @parts; @{ shift @parts } },
   );
   $f->on_fail( sub { die @_ } );

   await_multipart_initiate_and_respond "six";

   foreach ( [ 1, "Content generated " ], [ 2, "by code" ] ) {
      await_multipart_part_and_respond "six", @$_;
   }

   await_multipart_complete_and_respond "six";

   wait_for { $f->is_ready };

   my ( $etag, $len ) = $f->get;
   is( $len, 25, '$length from multipart put from CODE' );
}

# Multipart put from value automatically split
{
   $s3->configure( part_size => 16 );

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one.split",
      value  => "Content too long for one chunk",
   );
   $f->on_fail( sub { die @_ } );

   await_multipart_initiate_and_respond "one.split";

   foreach ( [ 1, "Content too long" ], [ 2, " for one chunk" ] ) {
      await_multipart_part_and_respond "one.split", @$_;
   }

   await_multipart_complete_and_respond "one.split";

   wait_for { $f->is_ready };

   my ( $etag ) = $f->get;
   is( $etag, '"3858f62230ac3c915f300c664312c11f-2"', 'result of multipart put from value' );

   # Restore it
   $s3->configure( part_size => 100*1024*1024 );
}

# Multipart put with extra metadata
{
   my @parts = ( "Part #1", "Part #2" );

   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "FOUR",
      gen_parts => sub { return unless @parts; shift @parts },
      meta   => {
         A      => "a",
      },
   );
   $f->on_fail( sub { die @_ } );

   my ( $request ) = await_multipart_initiate_and_respond "FOUR";
   is( $request->header( "X-Amz-Meta-A" ), "a", '$request has X-Amz-Meta-A header' );

   # Now wait on the chunks
   foreach ( [ 1, "Part #1" ], [ 2, "Part #2" ] ) {
      await_multipart_part_and_respond "FOUR", @$_;
   }

   await_multipart_complete_and_respond "FOUR";

   wait_for { $f->is_ready };
   $f->get;
}

# Multipart put with no actual parts
{
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "FIVE",
      gen_parts => sub { () },
   );
   $f->on_fail( sub { die @_ } );

   my $req;
   wait_for { $req = $http->pending_request };

   is( $req->method, "PUT" );

   my $md5 = md5_hex( $req->content );

   $http->respond(
      HTTP::Response->new( 200, "OK", [
            ETag => qq("$md5"),
         ], "" )
   );

   wait_for { $f->is_ready };
   $f->get;
}

# Test that a configured timeout is only used for meta-operations
{
   my $f;
   my $req;
   my @parts;

   $s3->configure( timeout => 10 );

   @parts = ( "Part #1", "Part #2" );
   $f = $s3->put_object(
      bucket => "bucket",
      key    => "-four-",
      gen_parts => sub { return unless @parts; shift @parts },
   );
   $f->on_fail( sub { die @_ } );

   ( $req ) = await_multipart_initiate_and_respond "-four-";
   is( $req->header( "X-NaHTTP-Timeout" ), 10, 'Initiate request has timeout set for configured' );

   # Now wait on the chunks
   foreach ( [ 1, "Part #1" ], [ 2, "Part #2" ] ) {
      ( $req ) = await_multipart_part_and_respond "-four-", @$_;
      is( $req->header( "X-NaHTTP-Timeout" ), undef, 'Part request has no timeout for configured' );
   }

   ( $req ) = await_multipart_complete_and_respond "-four-";
   is( $req->header( "X-NaHTTP-Timeout" ), 10, 'Complete request has timeout set for configured' );
   wait_for { $f->is_ready };
   $f->get;

   # Now, direct arguments
   @parts = ( "Part #1", "Part #2" );
   $f = $s3->put_object(
      bucket => "bucket",
      key    => "-four-",
      gen_parts => sub { return unless @parts; shift @parts },
      part_timeout => 30,
      meta_timeout => 20,
   );
   $f->on_fail( sub { die @_ } );

   ( $req ) = await_multipart_initiate_and_respond "-four-";
   is( $req->header( "X-NaHTTP-Timeout" ), 20, 'Initiate request has timeout set for direct' );

   # Now wait on the chunks
   foreach ( [ 1, "Part #1" ], [ 2, "Part #2" ] ) {
      ( $req ) = await_multipart_part_and_respond "-four-", @$_;
      is( $req->header( "X-NaHTTP-Timeout" ), 30, 'Part request timeout set for direct' );
   }

   ( $req ) = await_multipart_complete_and_respond "-four-";
   is( $req->header( "X-NaHTTP-Timeout" ), 20, 'Complete request has timeout set for direct' );


   wait_for { $f->is_ready };
   $f->get;
}

done_testing;
