package TestHTTP;

use strict;
use warnings;

use Future;
use Scalar::Util qw( blessed );

my $pending_request;
my $pending_request_content;
my $pending_request_on_write;
my $pending_response;
my $pending_on_header;
my $pending_on_chunk;

sub new { bless [], shift }

sub _pull_content
{
   my ( $content, $request ) = @_;

   if( !ref $content ) {
      $request->add_content( $content );
      $pending_request_on_write->( length $content ) if $pending_request_on_write;
   }
   elsif( ref $content eq "CODE" ) {
      while( defined( my $chunk = $content->() ) ) {
         _pull_content( $chunk, $request );
      }
   }
   elsif( blessed $content and $content->isa( "Future" ) ) {
      $content->on_done( sub {
         my ( $chunk ) = @_;
         _pull_content( $chunk, $request );
      });
   }
   else {
      die "TODO: Not sure how to handle $content";
   }
}

sub do_request
{
   shift; # self
   my %args = @_;

   defined $pending_request and die "Already have a pending request";

   $pending_request = delete $args{request};
   $pending_request_content = delete $args{request_body};
   if( my $on_body_write = delete $args{on_body_write} ) {
      my $written = 0;
      $pending_request_on_write = sub {
         $on_body_write->( $written += $_[0] );
      };
   }
   $pending_on_header = delete $args{on_header};
   if( my $timeout = delete $args{timeout} ) {
      # Cheat - easier for the unit tests to find it here
      $pending_request->header( "X-NaHTTP-Timeout" => $timeout );
   }

   delete $args{expect_continue};
   delete $args{SSL};

   delete $args{stall_timeout};

   die "TODO: more args: " . join( ", ", keys %args ) if keys %args;

   return $pending_response = Future->new;
}

sub pending_request
{
   if( $pending_request_content ) {
      _pull_content( $pending_request_content, $pending_request );
      undef $pending_request_content;
   }

   return $pending_request;
}

sub pending_request_plus_content
{
   return $pending_request, $pending_request_content;
}

sub respond
{
   shift;
   my ( $response ) = @_;

   my $f = $pending_response;

   undef $pending_request;
   undef $pending_response;

   if( $pending_on_header ) {
      my $header = $response->clone;
      $header->content("");

      my $on_chunk = $pending_on_header->( $header );
      $on_chunk->( $response->content );
      $f->done( $on_chunk->() );
   }
   else {
      $f->done( $response );
   }
}

sub respond_header
{
   shift;
   my ( $header ) = @_;

   $pending_on_chunk = $pending_on_header->( $header );
}

sub respond_more
{
   shift;
   my ( $chunk ) = @_;

   $pending_on_chunk->( $chunk );
}

sub respond_done
{
   shift;

   my $f = $pending_response;

   undef $pending_request;
   undef $pending_response;

   $f->done( $pending_on_chunk->() );
}

0x55AA;
