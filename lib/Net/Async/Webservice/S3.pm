#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::Webservice::S3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

our $VERSION = '0.14';

use Carp;

use Digest::HMAC_SHA1;
use Digest::MD5 qw( md5 );
use Future 0.18; # ->wrap, bugfgix to fmap* on immediates
use Future::Utils 0.16 qw( repeat try_repeat );
use HTTP::Date qw( time2str );
use HTTP::Request;
use IO::Async::Timer::Countdown;
use MIME::Base64 qw( encode_base64 );
use Scalar::Util qw( blessed );
use URI::Escape qw( uri_escape_utf8 );
use XML::LibXML;
use XML::LibXML::XPathContext;

my $libxml = XML::LibXML->new;

use constant DEFAULT_S3_HOST => "s3.amazonaws.com";

=head1 NAME

C<Net::Async::Webservice::S3> - use Amazon's S3 web service with C<IO::Async>

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::Webservice::S3;

 my $loop = IO::Async::Loop->new;

 my $s3 = Net::Async::Webservice::S3->new(
    access_key => ...,
    secret_key => ...,
    bucket     => "my-bucket-here",
 );
 $loop->add( $s3 );

 my $put_f = $s3->put_object(
    key   => "the-key",
    value => "A new value for the key\n";
 );

 my $get_f = $s3->get_object(
    key => "another-key",
 );

 $loop->await_all( $put_f, $get_f );

 print "The value is:\n", $get_f->get;

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
      Net::Async::HTTP->VERSION( '0.29' ); # on_body_write
      my $http = Net::Async::HTTP->new;
      $self->add_child( $http );
      $http;
   };

   $args->{max_retries} //= 3;
   $args->{list_max_keys} //= 1000;
   $args->{read_size} //= 64*1024; # 64 KiB

   # S3 docs suggest > 100MB should use multipart. They don't actually
   # document what size of parts to use, but we'll use that again.
   $args->{part_size} //= 100*1024*1024;

   $args->{host} //= DEFAULT_S3_HOST;

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

=item ssl => BOOL

Optional. If given a true value, will use C<https> URLs over SSL. Defaults to
off. This feature requires the optional L<IO::Async::SSL> module if using
L<Net::Async::HTTP>.

=item bucket => STRING

Optional. If supplied, gives the default bucket name to use, at which point it
is optional to supply to the remaining methods.

=item prefix => STRING

Optional. If supplied, this prefix string is prepended to any key names passed
in methods, and stripped from the response from C<list_bucket>. It can be used
to keep operations of the object contained within the named key space. If this
string is supplied, don't forget that it should end with the path delimiter in
use by the key naming scheme (for example C</>).

=item host => STRING

Optional. Sets the hostname to talk to the S3 service. Usually the default of
C<s3.amazonaws.com> is sufficient. This setting allows for communication with
other service providers who provide the same API as S3.

=item max_retries => INT

Optional. Maximum number of times to retry a failed operation. Defaults to 3.

=item list_max_keys => INT

Optional. Maximum number of keys at a time to request from S3 for the
C<list_bucket> method. Larger values may be more efficient as fewer roundtrips
will be required per method call. Defaults to 1000.

=item part_size => INT

Optional. Size in bytes to break content for using multipart upload. If an
object key's size is no larger than this value, multipart upload will not be
used. Defaults to 100 MiB.

=item read_size => INT

Optional. Size in bytes to read per call to the C<$gen_value> content
generation function in C<put_object>. Defaults to 64 KiB. Be aware that too
large a value may lead to the C<PUT> stall timer failing to be invoked on slow
enough connections, causing spurious timeouts.

=item timeout => NUM

Optional. If configured, this is passed into individual requests of the
underlying C<Net::Async::HTTP> object, except for the actual content C<GET> or
C<PUT> operations. It is therefore used by C<list_bucket>, C<delete_object>,
and the multi-part metadata operations used by C<put_object>. To apply an
overall timeout to an individual C<get_object> or C<put_object> operation,
pass a specific C<timeout> argument to those methods specifically.

=item stall_timeout => NUM

Optional. If configured, this is passed into the underlying
C<Net::Async::HTTP> object and used for all content uploads and downloads.

=back

=cut

sub configure
{
   my $self = shift;
   my %args = @_;

   foreach (qw( http access_key secret_key ssl bucket prefix host max_retries
                list_max_keys part_size read_size timeout stall_timeout )) {
      exists $args{$_} and $self->{$_} = delete $args{$_};
   }

   $self->SUPER::configure( %args );
}

=head1 METHODS

The following methods all support the following common arguments:

=over 8

=item timeout => NUM

=item stall_timeout => NUM

Optional. Passed directly to the underlying C<< Net::Async::HTTP->request >>
method.

=back

Each method below that yields a C<Future> is documented in the form

 $s3->METHOD( ARGS ) ==> YIELDS

Where the C<YIELDS> part indicates the values that will eventually be returned
by the C<get> method on the returned Future object, if it succeeds.

=cut

sub _make_request
{
   my $self = shift;
   my %args = @_;

   my $method = $args{method};
   defined $args{content} or croak "Missing 'content'";

   my @params;
   foreach my $key ( sort keys %{ $args{query_params} } ) {
      next unless defined( my $value = $args{query_params}->{$key} );
      $key =~ s/_/-/g;
      push @params, $key . "=" . uri_escape_utf8( $value, "^A-Za-z0-9_-" );
   }

   my $bucket = $args{bucket} // $self->{bucket};
   my $path   = $args{abs_path} // join "", grep { defined } $self->{prefix}, $args{path};

   my $scheme = $self->{ssl} ? "https" : "http";

   my $uri;
   if( length $bucket <= 63 and $bucket =~ m{^[A-Z0-9][A-Z0-9.-]+$}i ) {
      $uri = "$scheme://$bucket.$self->{host}/$path";
   }
   else {
      $uri = "$scheme://$self->{host}/$bucket/$path";
   }
   $uri .= "?" . join( "&", @params ) if @params;

   my $s3 = $self->{s3};

   my $meta = $args{meta} || {};

   my @headers = (
      Date => time2str( time ),
      %{ $args{headers} || {} },
      ( map { +"X-Amz-Meta-$_" => $meta->{$_} } sort keys %$meta ),
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
   if( defined $request->uri->query and length $request->uri->query ) {
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

   my %x_amz_headers;
   $request->scan( sub {
      $x_amz_headers{lc $_[0]} = $_[1] if $_[0] =~ m/^X-Amz-/i;
   });

   my $x_amz_headers = "";
   $x_amz_headers .= "$_:$x_amz_headers{$_}\n" for sort keys %x_amz_headers;

   my $buffer = join( "\n",
      $request->method,
      $request->header( "Content-MD5" ) // "",
      $request->header( "Content-Type" ) // "",
      $request->header( "Date" ) // "",
      $x_amz_headers . $canon_resource );

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

   $self->{http}->do_request(
      request => $request,
      SSL => ( $request->uri->scheme eq "https" ),
      %args
   )->and_then( sub {
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
   my ( $request, @args ) = @_;

   $self->_do_request( $request, @args )->then( sub {
      my $resp = shift;

      my $xpc = XML::LibXML::XPathContext->new( $libxml->parse_string( $resp->content ) );
      $xpc->registerNs( s3 => "http://s3.amazonaws.com/doc/2006-03-01/" );

      return Future->wrap( $xpc );
   });
}

sub _retry
{
   my $self = shift;
   my ( $method, @args ) = @_;

   my $delay = 0.5;

   my $retries = $self->{max_retries};
   try_repeat {
      my ( $prev_f ) = @_;

      # Add a small delay after failure before retrying
      my $delay_f =
         $prev_f ? $self->loop->delay_future( after => ( $delay *= 2 ) )
                 : Future->new->done;

      $delay_f->then( sub { $self->$method( @args ) } );
   } while => sub {
      my $f = shift;
      my ( $failure, $request, $response ) = $f->failure or return 0; # success
      return 0 if $response and $response->code =~ m/^4/; # don't retry HTTP 4xx
      return --$retries;
   };
}

=head2 $s3->list_bucket( %args ) ==> ( $keys, $prefixes )

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
         content      => "",
      );

      $self->_do_request_xpc( $req,
         timeout       => $args{timeout} // $self->{timeout},
         stall_timeout => $args{stall_timeout} // $self->{stall_timeout},
      )->then( sub {
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
            return Future->wrap( $last_key );
         }
         return Future->new->done;
      });
   } while => sub {
      my $f = shift;
      !$f->failure and $f->get };

   $f->then( sub {
      return Future->wrap( \@keys, \@prefixes );
   });
}

sub list_bucket
{
   my $self = shift;
   $self->_retry( "_list_bucket", @_ );
}

=head2 $s3->get_object( %args ) ==> ( $value, $response, $meta )

Requests the value of a key from a bucket.

Takes the following named arguments:

=over 8

=item bucket => STR

The name of the S3 bucket to query

=item key => STR

The name of the key to query

=item on_chunk => CODE

Optional. If supplied, this code will be invoked repeatedly on receipt of more
bytes of the key's value. It will be passed the L<HTTP::Response> object
received in reply to the request, and a byte string containing more bytes of
the value. Its return value is not important.

 $on_chunk->( $header, $bytes )

If this is supplied then the key's value will not be accumulated, and the
final result of the Future will be an empty string.

=back

The Future will return a byte string containing the key's value, the
L<HTTP::Response> that was received, and a hash reference containing any of
the metadata fields, if found in the response. If an C<on_chunk> code
reference is passed, the C<$value> string will be empty.

=cut

sub _head_then_get_object
{
   my $self = shift;
   my %args = @_;

   # TODO: This doesn't handle cancellation or retries
   # But that said neither does the rest of this module, wrt: on_chunk streaming

   my $on_chunk = delete $args{on_chunk};

   my $head_future = $self->loop->new_future;
   my $value_future;

   my $request = $self->_make_request(
      method  => $args{method},
      bucket  => $args{bucket},
      path    => $args{key},
      content => "",
   );

   $self->_do_request( $request,
      timeout       => $args{timeout},
      stall_timeout => $args{stall_timeout} // $self->{stall_timeout},
      on_header => sub {
         my ( $header ) = @_;
         my $code = $header->code;

         my %meta;
         $header->scan( sub {
            $_[0] =~ m/^X-Amz-Meta-(.*)$/i and $meta{$1} = $_[1];
         });

         $value_future = $head_future->new;
         $head_future->done( $value_future, $header, \%meta );

         return sub {
            if( @_ and $code == 200 ) {
               $on_chunk ? $on_chunk->( $header, @_ ) : $header->add_content( $_[0] );
               return;
            }
            return $header; # with no body content
         };
      }
   )->on_done( sub {
      my ( $response ) = @_;
      $value_future->done( $response->content, ( $head_future->get )[1,2] );
   })->on_fail( sub {
      ( $value_future || $head_future )->fail( @_ );
   });

   return $head_future;
}

sub get_object
{
   my $self = shift;
   my %args = @_;

   $self->_retry( sub {
      $self->_head_then_get_object( %args, method => "GET" )
         ->then( sub { my ( $value_f ) = @_; $value_f }); # wait on the value
   });
}

=head2 $s3->head_object( %args ) ==> ( $response, $meta )

Requests the value metadata of a key from a bucket. This is similar to the
C<get_object> method, but uses the C<HEAD> HTTP verb instead of C<GET>.

Takes the same named arguments as C<get_object>, but will ignore an
C<on_chunk> callback, if provided.

The Future will return the L<HTTP::Response> object and metadata hash
reference, without the content string (as no content is returned to a C<HEAD>
request).

=cut

sub head_object
{
   my $self = shift;
   my %args = @_;

   $self->_retry( sub {
      $self->_head_then_get_object( %args, method => "HEAD" )
         ->then( sub { my ( $value_f ) = @_; $value_f }) # wait on the empty body
         ->then( sub { shift; Future->wrap( @_ ) });     # remove (empty) value
   });
}

=head2 $s3->head_then_get_object( %args ) ==> ( $value_f, $response, $meta )

Performs a C<GET> operation similar to C<get_object>, but allows access to the
metadata header before the body content is complete.

Takes the same named arguments as C<get_object>.

The returned Future completes as soon as the metadata header has been received
and yields a second future (the body future), the L<HTTP::Response> and a hash
reference containing the metadata fields. The body future will eventually
yield the actual body, along with another copy of the response and metadata
hash reference.

 $value_f ==> $value, $response, $meta

=cut

sub head_then_get_object
{
   my $self = shift;
   $self->_retry( "_head_then_get_object", @_, method => "GET" );
}

=head2 $s3->put_object( %args ) ==> ( $etag, $length )

Sets a new value for a key in the bucket.

Takes the following named arguments:

=over 8

=item bucket => STRING

The name of the S3 bucket to put the value in

=item key => STRING

The name of the key to put the value in

=item value => STRING

=item value => Future giving STRING

Optional. If provided, gives a byte string as the new value for the key or a
L<Future> which will eventually yield such.

=item value => CODE

=item value_length => INT

Alternative form of C<value>, which is a C<CODE> reference to a generator
function. It will be called repeatedly to generate small chunks of content,
being passed the position and length it should yield.

 $chunk = $value->( $pos, $len )

Typically this can be provided either by a C<substr> operation on a larger
string buffer, or a C<sysseek> and C<sysread> operation on a filehandle.

In normal operation the function will just be called in a single sweep in
contiguous regions up to the extent given by C<value_length>. If however, the
MD5sum check fails at the end of upload, it will be called again to retry the
operation. The function must therefore be prepared to be invoked multiple
times over its range.

=item gen_parts => CODE

Alternative to C<value> in the case of larger values, and implies the use of
multipart upload. Called repeatedly to generate successive parts of the
upload. Each time C<gen_parts> is called it should return either a byte string
containing the value for that part, a L<Future> which will eventually yield a
byte string, or a 2-element list consisting of a CODE reference to the part's
generator function and the length in bytes that it will eventually yield.

 ( $value ) = $gen_parts->()

 ( $value_f ) = $gen_parts->(); $value = $value_f->get

 ( $gen_value, $value_length ) = $gen_parts->()

Each case is analogous to the types that the C<value> key can take.

=item meta => HASH

Optional. If provided, gives additional user metadata fields to set on the
object, using the C<X-Amz-Meta-> fields.

=item timeout => NUM

Optional. For single-part uploads, this sets the C<timeout> argument to use
for the actual C<PUT> request. For multi-part uploads, this argument is
currently ignored.

=item meta_timeout => NUM

Optional. For multipart uploads, this sets the C<timeout> argument to use for
the initiate and complete requests, overriding a configured C<timeout>.
Ignored for single-part uploads.

=item part_timeout => NUM

Optional. For multipart uploads, this sets the C<timeout> argument to use for
the individual part C<PUT> requests. Ignored for single-part uploads.

=item on_write => CODE

Optional. If provided, this code will be invoked after each successful
C<syswrite> call on the underlying filehandle, indicating that the data was at
least written as far as the kernel. It will be passed the total byte length
that has been written for this call to C<put_object>.

 $on_write->( $written )

Note that because of retries it is possible this count will decrease, if a
part has to be retried due to e.g. a failing MD5 checksum.

=back

The Future will return a string containing the S3 ETag of the newly-set key,
and the length of the value in bytes.

For single-part uploads the ETag will be the MD5 sum in hex, surrounded by
quote marks. For multi-part uploads this is a string in a different form,
though details of its generation are not specified by S3.

The returned MD5 sum from S3 during upload will be checked against an
internally-generated MD5 sum of the content that was sent, and an error result
will be returned if these do not match.

=cut

sub _md5sum_wrap
{
   my $content = shift;
   my @args = my ( $md5ctx, $posref, $content_length, $read_size ) = @_;

   if( !defined $content or !ref $content ) {
      my $len = $content_length - $$posref;
      if( defined $content ) {
         substr( $content, $len ) = "" if length $content > $len;
      }
      else {
         $content = "\0" x $len;
      }

      $md5ctx->add( $content );
      $$posref += length $content;

      return $content;
   }
   elsif( ref $content eq "CODE" ) {
      return sub {
         return undef if $$posref >= $content_length;

         my $len = $content_length - $$posref;
         $len = $read_size if $len > $read_size;

         my $chunk = $content->( $$posref, $len );
         return _md5sum_wrap( $chunk, @args );
      }
   }
   elsif( blessed $content and $content->isa( "Future" ) ) {
      return $content->transform(
         done => sub {
            my ( $chunk ) = @_;
            return _md5sum_wrap( $chunk, @args );
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

   my $md5ctx = Digest::MD5->new;
   my $on_write = $args{on_write};
   my $on_write_offset = $args{on_write_offset} // 0;
   my $pos = 0;

   # Make $content definitely a Future
   Future->wrap( delete $args{content} )->then( sub {
      my ( $content ) = @_;
      my $content_length = ref $content ? $args{content_length} : length $content;
      defined $content_length or die "TODO: referential content $content needs length";

      my $request = $self->_make_request(
         %args,
         method  => "PUT",
         content => "", # Doesn't matter, it'll be ignored
      );

      $request->content_length( $content_length );
      $request->content( "" );

      $self->_do_request( $request,
         timeout       => $args{timeout},
         stall_timeout => $args{stall_timeout} // $self->{stall_timeout},
         expect_continue => 1,
         request_body => _md5sum_wrap( $content, $md5ctx, \$pos, $content_length, $self->{read_size} ),
         $on_write ? ( on_body_write => sub {
            $on_write->( $_[0] + $on_write_offset );
         } ) : (),
      );
   })->then( sub {
      my $resp = shift;

      defined( my $etag = $resp->header( "ETag" ) ) or
         return Future->new->die( "Response did not provide an ETag header" );

      # Amazon S3 currently documents that the returned ETag header will be
      # the MD5 hash of the content, surrounded in quote marks. We'd better
      # hope this continues to be true... :/
      my ( $got_md5 ) = lc($etag) =~ m/^"([0-9a-f]{32})"$/ or
         return Future->new->die( "Returned ETag ($etag) does not look like an MD5 sum", $resp );

      my $expect_md5 = lc($md5ctx->hexdigest);

      if( $got_md5 ne $expect_md5 ) {
         return Future->new->die( "Returned MD5 hash ($got_md5) did not match expected ($expect_md5)", $resp );
      }

      return Future->wrap( $etag, $pos );
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
      meta    => $args{meta},
   );

   $self->_do_request_xpc( $req,
      timeout       => $args{timeout},
      stall_timeout => $args{stall_timeout} // $self->{stall_timeout},
   )->then( sub {
      my $xpc = shift;

      my $id = $xpc->findvalue( ".//s3:InitiateMultipartUploadResult/s3:UploadId" );
      return Future->wrap( $id );
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

   $self->_do_request_xpc( $req,
      timeout       => $args{timeout},
      stall_timeout => $args{stall_timeout} // $self->{stall_timeout},
   )->then( sub {
      my $xpc = shift;

      my $etag = $xpc->findvalue( ".//s3:CompleteMultipartUploadResult/s3:ETag" );
      return Future->wrap( $etag );
   });
}

sub _put_object_multipart
{
   my $self = shift;
   my ( $gen_parts, %args ) = @_;

   my $on_write = $args{on_write};

   my @etags;

   $self->_retry( "_initiate_multipart_upload",
      timeout => $args{meta_timeout} // $self->{timeout},
      %args
   )->then( sub {
      my ( $id ) = @_;

      my $part_num = 0;
      my $total_len = 0;
      repeat {
         my ( $part_num, $content, %moreargs ) = @{$_[0]};

         $self->_retry( "_put_object",
            bucket       => $args{bucket},
            path         => $args{key},
            content      => $content,
            query_params => {
               partNumber   => $part_num,
               uploadId     => $id,
            },
            timeout      => $args{part_timeout},
            on_write     => $on_write,
            on_write_offset => $total_len,
            %moreargs,
         )->on_done( sub {
            my ( $etag, $len ) = @_;
            push @etags, [ $part_num, $etag ];
            $total_len += $len;
         })
      } generate => sub {
         my @content = $gen_parts->() or return;
         $part_num++;
         return [ $part_num, $content[0] ] if @content == 1;
         return [ $part_num, $content[0], content_length => $content[1] ] if @content == 2 and ref $content[0] eq "CODE";
         die "Unsure what to do with gen_part result @content";
      }, while => sub {
         !$_[0]->failure;
      }, otherwise => sub {
         return Future->wrap( $id, \@etags, $total_len );
      };
   })->then( sub {
      my ( $id, $etags, $len ) = @_;

      my $doc = XML::LibXML::Document->new( "1.0", "UTF-8" );
      $doc->addChild( my $root = $doc->createElement( "CompleteMultipartUpload" ) );

      #add content
      foreach ( @$etags ) {
         my ( $part_num, $etag ) = @$_;

         $root->addChild( my $part = $doc->createElement('Part') );
         $part->appendTextChild( PartNumber => $part_num );
         $part->appendTextChild( ETag       => $etag );
      }

      $self->_retry( "_complete_multipart_upload",
         %args,
         id      => $id,
         content => $doc->toString,
         timeout => $args{meta_timeout} // $self->{timeout},
      )->then( sub {
         my ( $etag ) = @_;
         return Future->wrap( $etag, $len );
      });
   });
}

sub put_object
{
   my $self = shift;
   my %args = @_;

   my $gen_parts;
   if( $gen_parts = delete $args{gen_parts} ) {
      # OK
   }
   else {
      my $content_length = $args{value_length} // length $args{value};

      my $part_size = $self->{part_size};

      if( $content_length > $part_size * 10_000 ) {
         croak "Cannot upload content in more than 10,000 parts - consider using a larger part_size";
      }
      elsif( $content_length > $part_size ) {
         $gen_parts = sub {
            return unless length $args{value};
            return substr( $args{value}, 0, $part_size, "" );
         };
      }
      else {
         my @parts = ( [ delete $args{value}, $content_length ] );
         $gen_parts = sub { return unless @parts; @{ shift @parts } };
      }
   }

   my @parts;
   push @parts, [ $gen_parts->() ];

   # Ensure first part is a Future then unfuture it
   Future->wrap( @{$parts[0]} )->then( sub {
      @{$parts[0]} = @_ if @_;

      push @parts, [ $gen_parts->() ];

      if( @{ $parts[1] } ) {
         # There are at least two parts; we'd better use multipart upload
         $self->_put_object_multipart( sub { @parts ? @{ shift @parts } : goto &$gen_parts }, %args );
      }
      elsif( @{ $parts[0] } ) {
         # There is exactly one part
         my ( $content, $content_length ) = @{ shift @parts };
         $self->_retry( "_put_object",
            bucket         => $args{bucket},
            path           => $args{key},
            content        => $content,
            content_length => $content_length,
            meta           => $args{meta},
            %args,
         );
      }
      else {
         # There are no parts at all - create an empty object
         $self->_retry( "_put_object",
            bucket  => $args{bucket},
            path    => $args{key},
            content => "",
            meta    => $args{meta},
            %args,
         );
      }
   });
}

=head2 $s3->delete_object( %args ) ==> ()

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

   $self->_do_request( $request,
      timeout       => $args{timeout} // $self->{timeout},
      stall_timeout => $args{stall_timeout} // $self->{stall_timeout},
   )->then( sub {
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
