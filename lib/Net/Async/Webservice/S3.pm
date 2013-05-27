#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::Webservice::S3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

our $VERSION = '0.01';

use Carp;

use Digest::HMAC_SHA1;
use Digest::MD5;
use Future::Utils qw( repeat );
use HTTP::Date qw( time2str );
use HTTP::Request;
use MIME::Base64 qw( encode_base64 );
use URI::Escape qw( uri_escape_utf8 );
use XML::LibXML;
use XML::LibXML::XPathContext;

my $libxml = XML::LibXML->new;

=head1 NAME

C<Net::Async::Webservice::S3> - use Amazon's S3 web service with C<IO::Async>

=head1 SYNOPSIS

 TODO

=head1 DESCRIPTION

This module provides a webservice API around Amazon's S3 web service for use
in an L<IO::Async>-based program. Each S3 operation is represented by a method
that returns a L<Future>; this future, if successful, will eventually return
the result of the operation.

=cut

sub _init
{
   my $self = shift;
   my ( $args ) = @_;

   $args->{http} ||= do {
      require Net::Async::HTTP;
      my $http = Net::Async::HTTP->new;
      $self->add_child( $http );
      $http;
   };

   $args->{max_retries} //= 3;
   $args->{list_max_keys} //= 100;

   return $self->SUPER::_init( @_ );
}

=head1 PARAMETERS

The following named parameters may be passed to C<new> or C<configure>:

=over 8

=item http => Net::Async::HTTP

Optional. Allows the caller to provide a specific asynchronous HTTP user agent
object to use. This will be invoked with a single method, as documented by
L<Net::Async::HTTP>:

 $response_f = $http->do_request( request => $request, ... )

If absent, a new instance will be created and added as a child notifier of
this object. If a value is supplied, it will be used as supplied and I<not>
specifically added as a child notifier. In this case, the caller must ensure
it gets added to the underlying L<IO::Async::Loop> instance, if required.

=item access_key => STRING

=item secret_key => STRING

The twenty-character Access Key ID and forty-character Secret Key to use for
authenticating requests to S3.

=item max_retries => INT

Optional. Maximum number of times to retry a failed operation. Defaults to 3.

=item list_max_keys => INT

Optional. Maximum number of keys at a time to request from S3 for the
C<list_bucket> method. Larger values may be more efficient as fewer roundtrips
will be required per method call. Defaults to 100.

=back

=cut

sub configure
{
   my $self = shift;
   my %args = @_;

   foreach (qw( http access_key secret_key max_retries list_max_keys )) {
      defined $args{$_} and $self->{$_} = delete $args{$_};
   }

   $self->SUPER::configure( %args );
}

=head1 METHODS

=cut

sub _make_request
{
   my $self = shift;
   my %args = @_;

   my $method = $args{method};

   my @params;
   foreach my $key ( sort keys %{ $args{query_params} } ) {
      next unless defined( my $value = $args{query_params}->{$key} );
      $key =~ s/_/-/g;
      push @params, $key . "=" . uri_escape_utf8( $value, "^A-Za-z0-9_-" );
   }

   my $bucket = $args{bucket};
   my $path   = $args{path};

   my $uri;
   # TODO: https?
   if( 1 ) { # TODO: sanity-check bucket
      $uri = "http://$bucket.s3.amazonaws.com/$path";
   }
   else {
      $uri = "http://s3.amazonaws.com/$bucket/$path";
   }
   $uri .= "?" . join( "&", @params ) if @params;

   my $s3 = $self->{s3};

   my @headers = (
      Date => time2str( time ),
   );

   my $request = HTTP::Request->new( $method, $uri, \@headers, $args{content} );

   $self->_gen_auth_header( $request, $bucket, $path );

   return $request;
}

sub _gen_auth_header
{
   my $self = shift;
   my ( $request, $bucket, $path ) = @_;

   # See also
   #   http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader

   my $canon_resource = "/$bucket/$path";

   my $buffer = join( "\n",
      $request->method,
      $request->header( "Content-MD5" ) // "",
      $request->header( "Content-Type" ) // "",
      $request->header( "Date" ) // "",
      # No AMZ headers
      $canon_resource );

   my $s3 = $self->{s3};

   my $hmac = Digest::HMAC_SHA1->new( $self->{secret_key} );
   $hmac->add( $buffer );

   my $access_key = $self->{access_key};
   my $authkey = encode_base64( $hmac->digest );
   # Trim the trailing \n
   $authkey =~ s/\n$//;

   $request->header( Authorization => "AWS $access_key:$authkey" );
}

# Turn non-2xx results into errors
sub _do_request
{
   my $self = shift;
   my ( $request, %args ) = @_;

   $self->{http}->do_request( request => $request, %args )->and_then( sub {
      my $f = shift;
      my $resp = $f->get;

      my $code = $resp->code;
      if( $code !~ m/^2/ ) {
         my $message = $resp->message;
         $message =~ s/\r$//; # HTTP::Response leaves the \r on this

         return Future->new->die(
            "$code $message on " . $request->method . " ". $request->uri->path,
            $request,
            $resp,
         );
      }

      return $f;
   });
}

# Convert response into an XML XPathContext tree
sub _do_request_xpc
{
   my $self = shift;
   my ( $request ) = @_;

   $self->_do_request( $request )->then( sub {
      my $resp = shift;

      my $xpc = XML::LibXML::XPathContext->new( $libxml->parse_string( $resp->content ) );
      $xpc->registerNs( s3 => "http://s3.amazonaws.com/doc/2006-03-01/" );

      return Future->new->done( $xpc );
   });
}

sub _retry
{
   my $self = shift;
   my ( $method, @args ) = @_;

   my $retries = $self->{max_retries};
   repeat {
      $self->$method( @args )
   } while => sub { shift->failure and --$retries },
     return => $self->loop->new_future;
}

=head2 $f = $s3->list_bucket( %args )

Requests a list of the keys in a bucket, optionally within some prefix.

Takes the following named arguments:

=over 8

=item bucket => STR

The name of the S3 bucket to query

=item prefix => STR

=item delimiter => STR

Optional. If supplied, the prefix and delimiter to use to divide up the key
namespace. Keys will be divided on the C<delimiter> parameter, and only the
key space beginning with the given prefix will be queried.

=back

The Future will return two ARRAY references. The first provides a list of the
keys found within the given prefix, and the second will return a list of the
common prefixes of further nested keys.

 ( $keys, $prefixes ) = $f->get

Each key in the C<$keys> list is given in a HASH reference containing

=over 8

=item key => STRING

The key's name

=item last_modified => STRING

The last modification time of the key given in ISO 8601 format

=item etag => STRING

The entity tag of the key

=item size => INT

The size of the key's value, in bytes

=item storage_class => STRING

The S3 storage class of the key

=back

Each key in the C<$prefixes> list is given as a plain string.

=cut

sub _list_bucket
{
   my $self = shift;
   my %args = @_;

   my @keys;
   my @prefixes;

   my $f = repeat {
      my ( $prev_f ) = @_;

      my $marker = $prev_f ? $prev_f->get : undef;

      my $req = $self->_make_request(
         method       => "GET",
         bucket       => $args{bucket},
         path         => "",
         query_params => {
            prefix       => $args{prefix},
            delimiter    => $args{delimiter},
            marker       => $marker,
            max_keys     => $self->{list_max_keys},
         },
      );

      $self->_do_request_xpc( $req )->then( sub {
         my $xpc = shift;

         foreach my $node ( $xpc->findnodes( ".//s3:Contents" ) ) {
            push @keys, {
               key           => $xpc->findvalue( ".//s3:Key", $node ),
               last_modified => $xpc->findvalue( ".//s3:LastModified", $node ),
               etag          => $xpc->findvalue( ".//s3:ETag", $node ),
               size          => $xpc->findvalue( ".//s3:Size", $node ),
               storage_class => $xpc->findvalue( ".//s3:StorageClass", $node ),
            };
         }

         foreach my $node ( $xpc->findnodes( ".//s3:CommonPrefixes" ) ) {
            push @prefixes, $xpc->findvalue( ".//s3:Prefix", $node );
         }

         if( $xpc->findvalue( ".//s3:IsTruncated" ) eq "true" ) {
            my $last_key = $keys[-1]->{key};
            return Future->new->done( $last_key );
         }
         return Future->new->done;
      });
   } while => sub {
      my $f = shift;
      !$f->failure and $f->get },
   return => $self->loop->new_future;

   $f->then( sub {
      return Future->new->done( \@keys, \@prefixes );
   });
}

sub list_bucket
{
   my $self = shift;
   $self->_retry( "_list_bucket", @_ );
}

=head2 $f = $s3->get_object( %args )

Requests the value of a key from a bucket.

Takes the following named arguments:

=over 8

=item bucket => STR

The name of the S3 bucket to query

=item key => STR

The name of the key to query

=item on_chunk => CODE

Optional. If supplied, this code will be invoked repeatedly on receipt of more
bytes of the key's value. It will be passed an L<HTTP::Response> object
containing the key's metadata, and a byte string containing more bytes of the
value. Its return value is not important.

 $on_chunk->( $header, $bytes )

If this is supplied then the key's value will not be accumulated, and the
final result of the Future will be an empty string.

=back

The Future will return a byte string containing the key's value and the
L<HTTP::Response> containing the key's metadata.

 ( $value, $response ) = $f->get;

If an C<on_chunk> code reference is passed, this string will be empty.

=cut

sub _get_object
{
   my $self = shift;
   my %args = @_;

   my $on_chunk = delete $args{on_chunk};

   my $request = $self->_make_request(
      method => "GET",
      bucket => $args{bucket},
      path   => $args{key},
   );

   my $get_f;
   if( $on_chunk ) {
      $get_f = $self->_do_request( $request,
         on_header => sub {
            my ( $header ) = @_;
            my $code = $header->code;

            return sub {
               return $on_chunk->( $header, @_ ) if @_ and $code == 200;
               return $header; # with no body content
            },
         }
      );
   }
   else {
      $get_f = $self->_do_request( $request );
   }

   return $get_f->then( sub {
      my $resp = shift;
      return Future->new->done( $resp->content, $resp );
   } );
}

sub get_object
{
   my $self = shift;
   $self->_retry( "_get_object", @_ );
}

=head2 $f = $s3->put_object( %args )

Sets a new value for a key in the bucket.

Takes the following named arguments:

=over 8

=item bucket => STRING

The name of the S3 bucket to put the value in

=item key => STRING

The name of the key to put the value in

=item value => STRING

Optional. If provided, gives a byte string as the new value for the key

=item gen_value => CODE

Alternative to C<value> in the case of larger values. Called repeatedly to
generate successive chunks of the value. Each invocation should return either
a string of bytes, or C<undef> to indicate completion.

=item value_length => INT

If C<gen_value> is given instead of C<value>, this argument must be provided
and gives the length of the data that C<gen_value> will eventually generate.

=back

The Future will return a single string containing the MD5 sum of the newly-set
key, as a 32-character hexadecimal string.

 ( $md5_hex ) = $f->get

The returned MD5 sum from S3 will be checked against an internally-generated
MD5 sum of the content that was sent, and an error result will be returned if
these do not match.

=cut

sub _put_object
{
   my $self = shift;
   my %args = @_;

   my $content_length = delete $args{value_length} // length $args{value};
   defined $content_length or croak "Require value_length or value";

   my $gen_value = delete $args{gen_value};
   my $value     = delete $args{value};

   my $request = $self->_make_request(
      method  => "PUT",
      bucket  => $args{bucket},
      path    => $args{key},
      content => "", # Doesn't matter, it'll be ignored
   );

   $request->content_length( $content_length );
   $request->content( "" );

   my $md5ctx = Digest::MD5->new;

   $self->_do_request( $request,
      request_body => sub {
         return undef if !$gen_value and !length $value;
         my $chunk = $gen_value ? $gen_value->() : substr( $value, 0, 64*1024, "" );
         return undef if !defined $chunk;

         $md5ctx->add( $chunk );
         return $chunk;
      },
   )->then( sub {
      my $resp = shift;

      defined( my $etag = $resp->header( "ETag" ) ) or
         return Future->new->die( "Response did not provide an ETag header" );

      # Amazon S3 currently documents that the returned ETag header will be
      # the MD5 hash of the content, surrounded in quote marks. We'd better
      # hope this continues to be true... :/
      my ( $got_md5 ) = $etag =~ m/^"([0-9a-f]{32})"$/ or
         return Future->new->die( "Returned ETag ($etag) does not look like an MD5 sum", $resp );

      my $expect_md5 = $md5ctx->hexdigest;

      if( $got_md5 ne $expect_md5 ) {
         return Future->new->die( "Returned MD5 hash ($got_md5) did not match expected ($expect_md5)", $resp );
      }

      return Future->new->done( $got_md5 );
   });
}

sub put_object
{
   my $self = shift;
   $self->_retry( "_put_object", @_ );
}

=head2 $f = $s3->delete_object( %args )

Deletes a key from the bucket.

Takes the following named arguments:

=over 8

=item bucket => STRING

The name of the S3 bucket to put the value in

=item key => STRING

The name of the key to put the value in

=back

The Future will return nothing.

=cut

sub _delete_object
{
   my $self = shift;
   my %args = @_;

   my $request = $self->_make_request(
      method  => "DELETE",
      bucket  => $args{bucket},
      path    => $args{key},
      content => "",
   );

   $self->_do_request( $request )->then( sub {
      return Future->new->done;
   });
}

sub delete_object
{
   my $self = shift;
   $self->_retry( "_delete_object", @_ );
}

=head1 SPONSORS

Parts of this code were paid for by

=over 2

=item *

Socialflow L<http://www.socialflow.com>

=item *

Shadowcat Systems L<http://www.shadow.cat>

=back

=head1 AUTHOR

Paul Evans <leonerd@leonerd.org.uk>

=cut

0x55AA;
