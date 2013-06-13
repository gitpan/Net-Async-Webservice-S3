package TestHTTP;

use strict;
use warnings;

use Future;
use Scalar::Util qw( blessed );

my $pending_request;
my $pending_response;
my $pending_on_header;
my $pending_on_chunk;

sub new { bless [], shift }

sub _pull_content
{
   my ( $content, $request ) = @_;

   if( !ref $content ) {
      $request->add_content( $content );
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
   $pending_on_header = delete $args{on_header};

   if( my $request_body = delete $args{request_body} ) {
      _pull_content( $request_body, $pending_request );
   }

   delete $args{expect_continue};

   die "TODO: more args: " . join( ", ", keys %args ) if keys %args;

   return $pending_response = Future->new;
}

sub pending_request
{
   return $pending_request;
}

sub respond
{
   shift;
   my ( $response ) = @_;

   my $f = $pending_response;

   undef $pending_request;
   undef $pending_response;

   $f->done( $response );
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
