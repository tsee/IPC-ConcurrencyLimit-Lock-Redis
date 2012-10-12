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

=head1 DESCRIPTION

This locking strategy uses L<Redis> to implement
locking ...

FIXME explain gotchas about leaving locks around...

=head1 METHODS

=head2 new

Given a hash ref with options, attempts to obtain a lock in
the pool. On success, returns the lock object, otherwise undef.

Required options:

=over 2

=item C<max_procs>

The maximum no. of locks (and thus usually processes)
to allow at one time.

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

