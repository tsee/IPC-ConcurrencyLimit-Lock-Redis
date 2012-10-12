package IPC::ConcurrencyLimit::Lock::Redis;
use 5.008001;
use strict;
use warnings;

our $VERSION = '0.01';

use Carp qw(croak);
use Redis;
use Redis::ScriptCache;
use Digest::SHA1 ();

use IPC::ConcurrencyLimit::Lock;
our @ISA = qw(IPC::ConcurrencyLimit::Lock);

use Class::XSAccessor
  getters => [qw(redis_conn max_procs key_name proc_info script_cache)];

# TODO optional expire
our $LuaScript_GetLock = q{
  local key = KEYS[1]
  local max_procs = ARGV[1]
  local proc_info = ARGV[2]
  local i

  for i = 1, max_procs, 1 do
    local x = redis.call('hexists', key, i)
    if x == 0 then
      redis.call('hset', key, i, proc_info)
      return i
    end
  end

  return 0
};
our $LuaScriptHash_GetLock = Digest::SHA1::sha1_hex($LuaScript_GetLock);

our $LuaScript_ReleaseLock = q{
  local key = KEYS[1]
  local lockno = ARGV[1]
  redis.call('hdel', key, lockno)
  return 1
};
our $LuaScriptHash_ReleaseLock = Digest::SHA1::sha1_hex($LuaScript_ReleaseLock);


sub new {
  my $class = shift;
  my $opt = shift;

  my $max_procs = $opt->{max_procs}
    or croak("Need a 'max_procs' parameter");
  my $redis_conn = $opt->{redis_conn}
    or croak("Need a 'redis_conn' parameter");
  my $key_name = $opt->{key_name}
    or croak("Need a 'key_name' parameter");

  my $sc = Redis::ScriptCache->new(redis_conn => $redis_conn);
  $sc->register_script(\$LuaScript_GetLock, $LuaScriptHash_GetLock);
  $sc->register_script(\$LuaScript_ReleaseLock, $LuaScriptHash_ReleaseLock);

  my $proc_info = $opt->{proc_info};
  $proc_info = time() if not defined $proc_info;
  my $self = bless {
    max_procs    => $max_procs,
    redis_conn   => $redis_conn,
    key_name     => $key_name,
    id           => undef,
    script_cache => $sc,
    proc_info    => $proc_info,
  } => $class;

  $self->_get_lock($key_name, $max_procs, $sc, $proc_info)
    or return undef;

  return $self;
}

sub _get_lock {
  my ($self, $key, $max_procs, $script_cache, $proc_info) = @_;

  my ($rv) = $script_cache->run_script(
    $LuaScriptHash_GetLock, [1, $key, $max_procs, $proc_info]
  );

  if (defined $rv and $rv > 0) {
    $self->{id} = $rv;
    return 1;
  }

  return();
}

sub _release_lock {
  my $self = shift;
  my $id = $self->id;
  return if not $id;

  $self->script_cache->run_script(
    $LuaScriptHash_ReleaseLock, [1, $self->key_name, $id]
  );

  $self->{id} = undef;
}

sub DESTROY {
  local $@;
  my $self = shift;
  $self->_release_lock();
}

1;

__END__


=head1 NAME

IPC::ConcurrencyLimit::Lock::Redis - Locking via Redis

=head1 SYNOPSIS

  # see also: IPC::ConcurrencyLimit::Lock
  
  use IPC::ConcurrencyLimit;
  use Redis;
  
  my $redis = Redis->new(server => ...);
  my $limit = IPC::ConcurrencyLimit->new(
    type       => 'Redis',
    max_procs  => 1, # defaults to 1
    redis_conn => $redis,
    key_name   => "mylock",
    # proc_info  => "...", # optional value to store. Default: time()
  );
  
  my $id = $limit->get_lock;
  if (not $id) {
    warn "Couldn't get lock";
    exit();
  }
  
  # do work

=head1 DESCRIPTION

This module requires a Redis server that supports Lua
scripting.

This locking strategy uses L<Redis> to implement an
C<IPC::ConcurrencyLimit> lock type. This particular
Redis-based lock implementation uses a single Redis
hash (a hash in a single Redis key) as storage for
tracking the locks.

=head2 Lock Implementation on the Server

The structure of the lock on the server is not considered an
implementation detail, but part of the public interface.
So long it is inspected and modified atomically, you can
choose to modify it through different channels than the API of
this class.

This is important because the lock is released in the
lock object's destructor, so if a perl process segfaults
or on network failure between the process and Redis
then the lock cannot be released! More on that below.

Given a lock C<"mylock"> with a C<max_procs> setting
of 5 (default: 1) and three out of five lock instances taken,
the lock structure in Redis would look as follows:

  "mylock": {
                "1": "some info",
                "2": "some other",
                "3": "yet other info"
            }

If subsequently lock number 2 is released, the structure
becomes:

  "mylock": {
                "1": "some info",
                "3": "yet other info"
            }

The next lock to be obtained would again use entry number 2.
When creating a lock object, you may pass a C<proc_info>
parameter. This parameter (string) will be used as the value
of the corresponding hash entry (C<"some info">, etc. above).
The C<proc_info> value defaults to the current epoch time
on the client.

The C<proc_info> properties may be used to evict stale locks
before attempting to obtain a lock. The default behaviour of
using the current time allows for expiring old locks if that
is good enough for your application. Using PIDs could be
used to clean out stale locks referring to the same client
host, etc.

=head1 METHODS

=head2 new

Given a hash ref with options, attempts to obtain a lock in
the pool. On success, returns the lock object, otherwise undef.

Required named parameters:

=over 2

=item C<max_procs>

The maximum no. of locks (and thus usually processes)
to allow at one time.

=item C<redis_conn>

A Redis connection object. See L<Redis>.

=item C<key_name>

Indicates the Redis key to use for storing the lock hash.

=back

Options:

=over 2

=item C<proc_info>

If provided, this string will be stored in the value slot for
the lock obtained. Defaults to current client time (C<time()>).

=back

=head1 AUTHOR

Steffen Mueller, C<smueller@cpan.org>

=head1 COPYRIGHT AND LICENSE

 (C) 2012 Steffen Mueller. All rights reserved.
 
 This code is available under the same license as Perl version
 5.8.1 or higher.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

=cut

