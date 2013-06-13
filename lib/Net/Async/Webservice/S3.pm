#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::Webservice::S3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

our $VERSION = '0.04';

use Carp;

use Digest::HMAC_SHA1;
use Digest::MD5 qw( md5 );
use Future 0.13; # ->then
use Future::Utils qw( repeat );
use HTTP::Date qw( time2str );
use HTTP::Request;
use MIME::Base64 qw( encode_base64 );
use Scalar::Util qw( blessed );
use URI::Escape qw( uri_escape_utf8 );
use XML::LibXML;
use XML::LibXML::XPathContext;

use constant READ_SIZE => 64*1024; # 64 KiB

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
      Net::Async::HTTP->VERSION( '0.19' ); # Futures
      my $http = Net::Async::HTTP->new;
      $self->add_child( $http );
      $http;
   };

   $args->{max_retries} //= 3;
   $args->{list_max_keys} //= 1000;

   # S3 docs suggest > 100MB should use multipart. They don't actually
   # document what size of parts to use, but we'll use that again.
   $args->{part_size} //= $args->{multipart_chunk_size} // 100*1024*1024;

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

=item bucket => STRING

Optional. If supplied, gives the default bucket name to use, at which point it
is optional to supply to the remaining methods.

=item prefix => STRING

Optional. If supplied, this prefix string is prepended to any key names passed
in methods, and stripped from the response from C<list_bucket>. It can be used
to keep operations of the object contained within the named key space. If this
string is supplied, don't forget that it should end with the path delimiter in
use by the key naming scheme (for example C</>).

=item max_retries => INT

Optional. Maximum number of times to retry a failed operation. Defaults to 3.

=item list_max_keys => INT

Optional. Maximum number of keys at a time to request from S3 for the
C<list_bucket> method. Larger values may be more efficient as fewer roundtrips
will be required per method call. Defaults to 1000.

=item part_size => INT

Optional. Size in bytes to break content for using multipart upload. If an object
key's size is no larger than this value, multipart upload will not be used.
Defaults to 100 MiB. (Used to be called C<multipart_chunk_size>.

=back

=cut

sub configure
{
   my $self = shift;
   my %args = @_;

   # Legacy name
   $args{part_size} = delete $args{multipart_chunk_size} if defined $args{multipart_chunk_size};

   foreach (qw( http access_key secret_key bucket prefix max_retries list_max_keys
                part_size )) {
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

   my $bucket = $args{bucket} // $self->{bucket};
   my $path   = $args{abs_path} // join "", grep { defined } $self->{prefix}, $args{path};

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
      %{ $args{headers} || {} },
   );

   my $request = HTTP::Request->new( $method, $uri, \@headers, $args{content} );
   $request->content_length( length $args{content} );

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
   if( length $request->uri->query ) {
      my %params = $request->uri->query_form;
      my %params_to_sign;
      foreach (qw( partNumber uploadId )) {
         $params_to_sign{$_} = $params{$_} if exists $params{$_};
      }

      my @params_to_sign;
      foreach ( sort keys %params_to_sign ) {
         push @params_to_sign, defined $params{$_} ? "$_=$params{$_}" : $_;
      }

      $canon_resource .= "?" . join( "&", @params_to_sign ) if @params_to_sign;
   }

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
   my $authkey = encode_base64( $hmac->digest, "" );

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
         abs_path     => "",
         query_params => {
            prefix       => join( "", grep { defined } $self->{prefix}, $args{prefix} ),
            delimiter    => $args{delimiter},
            marker       => $marker,
            max_keys     => $self->{list_max_keys},
         },
      );

      $self->_do_request_xpc( $req )->then( sub {
         my $xpc = shift;

         my $last_key;
         foreach my $node ( $xpc->findnodes( ".//s3:Contents" ) ) {
            my $key = $xpc->findvalue( ".//s3:Key", $node );
            $last_key = $key;

            $key =~ s/^\Q$self->{prefix}\E// if defined $self->{prefix};
            push @keys, {
               key           => $key,
               last_modified => $xpc->findvalue( ".//s3:LastModified", $node ),
               etag          => $xpc->findvalue( ".//s3:ETag", $node ),
               size          => $xpc->findvalue( ".//s3:Size", $node ),
               storage_class => $xpc->findvalue( ".//s3:StorageClass", $node ),
            };
         }

         foreach my $node ( $xpc->findnodes( ".//s3:CommonPrefixes" ) ) {
            my $key = $xpc->findvalue( ".//s3:Prefix", $node );

            $key =~ s/^\Q$self->{prefix}\E// if defined $self->{prefix};
            push @prefixes, $key;
         }

         if( $xpc->findvalue( ".//s3:IsTruncated" ) eq "true" ) {
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

=item gen_parts => CODE

Alternative to C<value> in the case of larger values, and implies the use of
multipart upload. Called repeatedly to generate successive parts of the
upload. Each time it is called it should yield two values; an integer giving
the length of this part, and a generator used to create it. The generator must
be a code reference of the same form as given in C<gen_value> below. When
there are no more parts it should return an empty list.

 ( $value_length, $gen_value ) = $gen_parts->()

The C<$pos> parameter to C<$gen_value> will begin at zero for each part.

=item gen_value => CODE

DEPRECATED. See instead C<gen_parts>

Alternative to C<value> in the case of larger values. Called repeatedly to
generate successive chunks of the value. Each invocation will be passed a byte
position and length, and should return a string of bytes.

 $bytes = $gen_value->( $pos, $length )

This callback will not be invoked on a range outside of the length given by
the C<value_length> parameter. It may return a shorter byte chunk than
requested.

If it declines entirely to yield enough content (by returning C<undef>) then
the remainder of the declared content length will be null-padded. If it
returns too much content, it will be truncated.

=item value_length => INT

If C<gen_value> is given instead of C<value>, this argument must be provided
and gives the length of the data that C<gen_value> will eventually generate.

=back

The Future will return a single string containing the S3 ETag of the newly-set
key. For single-part uploads this will be the MD5 sum in hex, surrounded by
quote marks. For multi-part uploads this is a string in a different form,
though details of its generation are not specified by S3.

 ( $etag ) = $f->get

The returned MD5 sum from S3 during upload will be checked against an
internally-generated MD5 sum of the content that was sent, and an error result
will be returned if these do not match.

=cut

sub _md5sum_wrap
{
   my ( $content, $md5ctx, $posref, $content_length ) = @_;

   if( !defined $content ) {
      $content = "\0" x ( $content_length - $$posref );

      $md5ctx->add( $content );
      $$posref += length $content;

      return $content;
   }
   elsif( !ref $content ) {
      my $len = $content_length - $$posref;
      substr( $content, $len ) = "" if length $content > $len;

      $md5ctx->add( $content );
      $$posref += length $content;

      return $content;
   }
   elsif( ref $content eq "CODE" ) {
      return sub {
         return undef if $$posref >= $content_length;

         my $len = $content_length - $$posref;
         $len = READ_SIZE if $len > READ_SIZE;

         my $chunk = $content->( $$posref, $len );
         return _md5sum_wrap( $chunk, $md5ctx, $posref, $content_length );
      }
   }
   elsif( blessed $content and $content->isa( "Future" ) ) {
      return $content->transform(
         done => sub {
            my ( $chunk ) = @_;
            return _md5sum_wrap( $chunk, $md5ctx, $posref, $content_length );
         },
      );
   }
   else {
      die "TODO: md5sum wrap ref $content";
   }
}

sub _put_object
{
   my $self = shift;
   my %args = @_;

   my $content_length = $args{content_length};
   my $gen_content    = $args{gen_content};

   my $request = $self->_make_request(
      %args,
      method  => "PUT",
      content => "", # Doesn't matter, it'll be ignored
   );

   $request->content_length( $content_length );
   $request->content( "" );

   my $md5ctx = Digest::MD5->new;

   my $pos = 0;

   $self->_do_request( $request,
      expect_continue => 1,
      request_body => _md5sum_wrap( $gen_content, $md5ctx, \$pos, $content_length ),
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

      return Future->new->done( $etag );
   });
}

sub _initiate_multipart_upload
{
   my $self = shift;
   my %args = @_;

   my $req = $self->_make_request(
      method  => "POST",
      bucket  => $args{bucket},
      path    => "$args{key}?uploads",
      content => "",
   );

   $self->_do_request_xpc( $req )->then( sub {
      my $xpc = shift;

      my $id = $xpc->findvalue( ".//s3:InitiateMultipartUploadResult/s3:UploadId" );
      return Future->new->done( $id );
   });
}

sub _complete_multipart_upload
{
   my $self = shift;
   my %args = @_;

   my $req = $self->_make_request(
      method       => "POST",
      bucket       => $args{bucket},
      path         => $args{key},
      content      => $args{content},
      query_params => {
         uploadId     => $args{id},
      },
      headers      => {
         "Content-Type" => "application/xml",
         "Content-MD5"  => encode_base64( md5( $args{content} ), "" ),
      },
   );

   $self->_do_request_xpc( $req )->then( sub {
      my $xpc = shift;

      my $etag = $xpc->findvalue( ".//s3:CompleteMultipartUploadResult/s3:ETag" );
      return Future->new->done( $etag );
   });
}

sub put_object
{
   my $self = shift;
   my %args = @_;

   my $use_multipart;
   my $gen_parts;
   if( $gen_parts = delete $args{gen_parts} ) {
      $use_multipart = 1;
   }
   else {
      my $content_length = delete $args{value_length} // length $args{value};
      defined $content_length or croak "Require value_length or value";

      my $part_size = $self->{part_size};

      my $gen_content;
      if( my $gen_value = delete $args{gen_value} ) {
         $gen_content = $gen_value;
      }
      elsif( my $value  = delete $args{value} ) {
         $gen_content = sub {
            return substr( $value, $_[0], $_[1] );
         };
      }
      else {
         croak "Require gen_value or value";
      }

      if( $content_length > $part_size * 10_000 ) {
         croak "Cannot upload content in more than 10,000 parts - consider using a larger part_size";
      }
      elsif( $content_length > $part_size ) {
         $use_multipart = 1;
         my $pos = 0;
         $gen_parts = sub {
            return if $pos >= $content_length;

            my $start = $pos;
            my $end = $pos + $part_size;
            $end = $content_length if $end > $content_length;

            $pos = $end;

            return ( $end - $start, sub {
               my ( $pos, $length ) = @_;
               $pos += $start;
               return $gen_content->( $pos, $length );
            });

            die "Yield a part starting at $pos until $end\n";
         };
      }
      else {
         $use_multipart = 0;
         $gen_parts = sub {
            return ( $content_length, $gen_content );
         };
      }
   }

   if( !$use_multipart ) {
      my ( $content_length, $gen_content ) = $gen_parts->();
      $self->_retry( "_put_object",
         bucket         => $args{bucket},
         path           => $args{key},
         content_length => $content_length,
         gen_content    => $gen_content,
      );
   }
   else {
      my @etags;

      $self->_retry( "_initiate_multipart_upload",
         %args
      )->then( sub {
         my ( $id ) = @_;

         my $part_num = 0;
         repeat {
            my ( $part_num, $content_length, $gen_content ) = @{$_[0]};

            $self->_retry( "_put_object",
               bucket         => $args{bucket},
               path           => $args{key},
               content_length => $content_length,
               gen_content    => $gen_content,
               query_params   => {
                  partNumber     => $part_num,
                  uploadId       => $id,
               },
            )->on_done( sub {
               my ( $etag ) = @_;
               push @etags, [ $part_num, $etag ];
            })
         } generate => sub {
            my ( $content_length, $gen_content ) = $gen_parts->() or return;
            return [ ++$part_num, $content_length, $gen_content ];
         }, while => sub {
            !$_[0]->failure;
         }, otherwise => sub {
            return Future->new->done( $id, @etags );
         }, return => $self->loop->new_future;
      })->then( sub {
         my ( $id, @etags ) = @_;

         my $doc = XML::LibXML::Document->new( "1.0", "UTF-8" );
         $doc->addChild( my $root = $doc->createElement( "CompleteMultipartUpload" ) );

         #add content
         foreach ( @etags ) {
            my ( $part_num, $etag ) = @$_;

            $root->addChild( my $part = $doc->createElement('Part') );
            $part->appendTextChild( PartNumber => $part_num );
            $part->appendTextChild( ETag       => $etag );
         }

         $self->_retry( "_complete_multipart_upload",
            %args,
            id      => $id,
            content => $doc->toString,
         );
      });
   }
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
