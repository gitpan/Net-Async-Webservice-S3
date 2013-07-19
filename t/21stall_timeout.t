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
   read_size => 4, # tiny chunks so we can test properly on small strings
   http => my $http = TestHTTP->new,
   access_key => 'K'x20,
   secret_key => 's'x40,
);

$loop->add( $s3 );

{
   # Need to PUT from a generator sub because otherwise the timeout logic
   # doesn't kick in
   my $content = "A new value";
   my $f = $s3->put_object(
      bucket => "bucket",
      key    => "one",
      value  => sub { substr( $content, $_[0], $_[1] ) },
      value_length => length $content,

      stall_timeout => 1,
   );

   my ( $req, $body );
   wait_for { ( $req, $body ) = $http->pending_request_plus_content; defined $req };

   ref $body eq "CODE" or bail( "Expected \$body to a CODE ref but it is not" );

   # Call it once only to get a 4-byte string
   is( length $body->(), 4, '$body->() yields a 4-byte string' );

   wait_for { $f->is_ready };

   is( scalar $f->failure, "Stalled during PUT", '$f fails' );
}

done_testing;
