// Package kvredis implements fh's kv.Provider/kv.Store contract
// (github.com/oarkflow/fh/pkg/storage/kv) on top of Redis, using
// github.com/redis/go-redis/v9.
//
// # Key layout
//
// Every entry is stored under <prefix><namespace>:<key>, prefix defaulting to
// "fh:". Each namespace also keeps one Redis sorted set,
// <prefix><namespace>:__index, that tracks every live key so Len can be
// answered without an O(n) key-space scan: ZADD member=key
// score=<absolute expiry as a Unix timestamp, or a far-future sentinel for
// entries with no TTL>. Get relies on Redis's own key TTL for actual
// expiration; the index is only consulted (and lazily pruned) by Len.
//
// # Client ownership
//
// New wraps an already-constructed, caller-owned *redis.Client: Close marks
// the Provider closed but never closes that client, because the caller may
// be sharing it with something else (this repo's sse/redis package is a
// realistic example of another consumer of the same client).
//
// Open builds and owns its own *redis.Client from a *redis.Options: Close
// closes that client too.
//
// # Mutate
//
// Mutate implements the atomic read-modify-write the kv.Store contract
// requires — atomic across every process sharing the Redis instance, not
// just within one Go process — using go-redis's WATCH/MULTI optimistic
// transaction pattern (see redis.Client.Watch). The watched GET, the
// caller's decision function, and the conditional SET+ZADD all happen inside
// one optimistic transaction that Redis aborts (redis.TxFailedErr) if the key
// changed underneath it; the transaction is retried until it succeeds or a
// bounded retry budget is exhausted.
package kvredis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

const (
	// defaultPrefix is prepended to every Redis key this package writes.
	defaultPrefix = "fh:"
	// indexSuffix names the per-namespace sorted-set key that tracks live
	// keys for Len.
	indexSuffix = ":__index"
	// maxNamespaceLen mirrors fh's own namespace validation.
	maxNamespaceLen = 128
	// maxMutateAttempts bounds the WATCH/MULTI optimistic-retry loop so a
	// pathologically hot key cannot spin Mutate forever.
	maxMutateAttempts = 1000
)

// noExpirySentinel is used as the sorted-set score for entries stored with no
// TTL, so ZREMRANGEBYSCORE(-inf, now) never prunes them. Year 9999 is "far
// enough" without risking overflow in downstream tooling that treats scores
// as ordinary timestamps.
var noExpirySentinel = float64(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC).Unix())

// config holds the options every namespace Store built by a Provider shares.
type config struct {
	prefix string
}

// Option configures a Provider constructed by New or Open.
type Option func(*config)

// WithPrefix overrides the default "fh:" key prefix. Empty values are
// ignored.
func WithPrefix(prefix string) Option {
	return func(c *config) {
		if prefix != "" {
			c.prefix = prefix
		}
	}
}

func newConfig(opts []Option) config {
	c := config{prefix: defaultPrefix}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	return c
}

// Provider is a Redis-backed kv.Provider. Construct it with New (caller-owned
// client) or Open (provider-owned client).
type Provider struct {
	client     *redis.Client
	ownsClient bool
	cfg        config

	mu     sync.Mutex
	stores map[string]*Store
	closed bool
}

var (
	_ kv.Provider = (*Provider)(nil)
	_ kv.Store    = (*Store)(nil)
)

// New returns a Provider that issues commands over client. The caller
// constructed and owns client: Provider.Close will mark the provider closed
// (subsequent Store calls return kv.ErrProviderClosed) but will NOT close
// client itself, since the caller may still be using it for other things
// (publishing to an sse/redis bus, for example).
func New(client *redis.Client, opts ...Option) *Provider {
	if client == nil {
		panic("kvredis: nil client")
	}
	return &Provider{
		client: client,
		cfg:    newConfig(opts),
		stores: make(map[string]*Store),
	}
}

// Open builds and owns its own *redis.Client from opt. It pings the server
// once to fail fast on misconfiguration. Provider.Close will close that
// client.
func Open(opt *redis.Options, opts ...Option) (*Provider, error) {
	client := redis.NewClient(opt)
	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("kvredis: connect: %w", err)
	}
	return &Provider{
		client:     client,
		ownsClient: true,
		cfg:        newConfig(opts),
		stores:     make(map[string]*Store),
	}, nil
}

// Store returns the isolated Redis-backed store for namespace, creating it
// on first use. Repeated calls with the same namespace return the same
// logical store (backed by the same keys); this is exported as a public
// entry point (satisfying kv.Provider) that fh's own namespace-validating
// caller is not the only path into, so namespace is validated here too.
func (p *Provider) Store(ctx context.Context, namespace string) (kv.Store, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if err := validateNamespace(namespace); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, kv.ErrProviderClosed
	}
	if s, ok := p.stores[namespace]; ok {
		return s, nil
	}
	s := &Store{
		client:    p.client,
		namespace: namespace,
		keyPrefix: p.cfg.prefix + namespace + ":",
		indexKey:  p.cfg.prefix + namespace + indexSuffix,
	}
	p.stores[namespace] = s
	return s, nil
}

// Close marks the provider closed (further Store calls return
// kv.ErrProviderClosed) and closes every namespace Store handle it has
// issued. It only closes the underlying *redis.Client if the provider owns
// it (i.e. it was built with Open). It is safe to call more than once.
func (p *Provider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	stores := make([]*Store, 0, len(p.stores))
	for _, s := range p.stores {
		stores = append(stores, s)
	}
	p.mu.Unlock()

	for _, s := range stores {
		_ = s.Close()
	}

	if p.ownsClient {
		return p.client.Close()
	}
	return nil
}

// Store is a namespaced, Redis-backed kv.Store. Obtain one from a Provider;
// do not construct it directly.
type Store struct {
	client    *redis.Client
	namespace string
	keyPrefix string
	indexKey  string

	mu     sync.RWMutex
	closed bool
}

func (s *Store) fullKey(key string) string {
	return s.keyPrefix + key
}

func (s *Store) checkClosed() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return kv.ErrClosed
	}
	return nil
}

// Get returns the value for key and true if present and not expired. Expiry
// is enforced by Redis's own key TTL, so a missing key and an expired key are
// indistinguishable here — both return (nil, false, nil).
func (s *Store) Get(key string) ([]byte, bool, error) {
	if err := s.checkClosed(); err != nil {
		return nil, false, err
	}
	ctx := context.Background()
	val, err := s.client.Get(ctx, s.fullKey(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("kvredis: get %q: %w", key, err)
	}
	return val, true, nil
}

// Set stores value for key with the given ttl (<=0 means no expiry),
// replacing any previous value, and records the key's new expiry in the
// namespace's Len index.
func (s *Store) Set(key string, value []byte, ttl time.Duration) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	ctx := context.Background()
	fk := s.fullKey(key)
	score := scoreForTTL(ttl)
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, fk, value, effectiveTTL(ttl))
		pipe.ZAdd(ctx, s.indexKey, redis.Z{Score: score, Member: key})
		return nil
	})
	if err != nil {
		return fmt.Errorf("kvredis: set %q: %w", key, err)
	}
	return nil
}

// Delete removes key from Redis and from the namespace's Len index. Deleting
// a missing key is not an error.
func (s *Store) Delete(key string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	ctx := context.Background()
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, s.fullKey(key))
		pipe.ZRem(ctx, s.indexKey, key)
		return nil
	})
	if err != nil {
		return fmt.Errorf("kvredis: delete %q: %w", key, err)
	}
	return nil
}

// Len reports the number of live entries in this namespace. Redis has no
// O(1) "count keys under a prefix" operation, so this is backed by a sorted
// set maintained on Set/Delete; Len first prunes members whose recorded
// expiry has passed (ZREMRANGEBYSCORE -inf now) and then returns ZCARD. Per
// the kv.Store contract this is allowed to be lazily consistent: a key that
// expired via Redis's native TTL since the last Len call is only reflected
// here once pruned, which happens on every Len call.
func (s *Store) Len() (int, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}
	ctx := context.Background()
	now := strconv.FormatInt(time.Now().Unix(), 10)
	if err := s.client.ZRemRangeByScore(ctx, s.indexKey, "-inf", now).Err(); err != nil {
		return 0, fmt.Errorf("kvredis: len prune: %w", err)
	}
	n, err := s.client.ZCard(ctx, s.indexKey).Result()
	if err != nil {
		return 0, fmt.Errorf("kvredis: len: %w", err)
	}
	return int(n), nil
}

// Close marks this Store handle closed; every other method then returns
// kv.ErrClosed. It affects only this handle: sibling Store handles for other
// namespaces obtained from the same Provider, and the shared *redis.Client,
// are untouched. Per fh's shared-state contract callers should close the
// Provider rather than individual stores, but the kv.Store interface
// requires the method regardless. Close is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// Mutate atomically loads the current value for key, passes it to fn, and —
// if fn reports ok=true — stores fn's returned value and ttl. The whole
// read-decide-write sequence is atomic across every process sharing this
// Redis instance (not just within one Go process), implemented with
// go-redis's WATCH/MULTI optimistic-transaction pattern: the GET is issued
// inside a watch on key's Redis entry, and the SET+ZADD are only committed if
// no other client wrote to that key in between. A concurrent writer causes
// Redis to abort the transaction (redis.TxFailedErr), which Mutate retries
// automatically up to a bounded number of attempts.
//
// If fn returns a non-nil error, Mutate aborts the transaction (no writes are
// made) and returns that error unwrapped, so callers can use errors.Is/As on
// it directly. If fn returns ok=false, the entry is left untouched (value and
// TTL unchanged) and Mutate returns nil.
func (s *Store) Mutate(key string, fn func(current []byte, exists bool) (next []byte, ttl time.Duration, ok bool, err error)) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	ctx := context.Background()
	fk := s.fullKey(key)

	var fnErr error
	txf := func(tx *redis.Tx) error {
		current, err := tx.Get(ctx, fk).Bytes()
		exists := true
		switch {
		case err == nil:
			// value present
		case errors.Is(err, redis.Nil):
			exists = false
			current = nil
		default:
			return fmt.Errorf("kvredis: mutate get %q: %w", key, err)
		}

		next, ttl, ok, err := fn(current, exists)
		if err != nil {
			fnErr = err
			return err
		}
		if !ok {
			return nil
		}

		score := scoreForTTL(ttl)
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, fk, next, effectiveTTL(ttl))
			pipe.ZAdd(ctx, s.indexKey, redis.Z{Score: score, Member: key})
			return nil
		})
		return err
	}

	for attempt := 0; attempt < maxMutateAttempts; attempt++ {
		fnErr = nil
		err := s.client.Watch(ctx, txf, fk)
		if fnErr != nil {
			return fnErr
		}
		if err == nil {
			return nil
		}
		if errors.Is(err, redis.TxFailedErr) {
			continue // optimistic lock lost to a concurrent writer; retry
		}
		return fmt.Errorf("kvredis: mutate %q: %w", key, err)
	}
	return fmt.Errorf("kvredis: mutate %q: exceeded %d attempts under contention", key, maxMutateAttempts)
}

// scoreForTTL returns the sorted-set score to record for an entry with the
// given ttl: its absolute expiry as a Unix timestamp, or a far-future
// sentinel for ttl<=0 (no expiry), so Len's pruning step never removes it.
func scoreForTTL(ttl time.Duration) float64 {
	if ttl <= 0 {
		return noExpirySentinel
	}
	return float64(time.Now().Add(ttl).Unix())
}

// effectiveTTL normalizes the kv.Store "<=0 means no expiry" convention to
// the go-redis "0 means no expiry" convention (go-redis treats any
// expiration <= 0 as "no TTL" internally, but callers should not rely on
// that: keep this explicit and centralized).
func effectiveTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	return ttl
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("kvredis: nil context")
	}
	return ctx.Err()
}

// validateNamespace defensively re-validates namespace the same way fh's own
// provider-request validation does, since Provider.Store is a public
// interface method that can be called directly rather than only through
// fh's namespace-validating caller.
func validateNamespace(namespace string) error {
	if namespace == "" || len(namespace) > maxNamespaceLen || !utf8.ValidString(namespace) || strings.TrimSpace(namespace) != namespace {
		return kv.ErrInvalidNamespace
	}
	for _, r := range namespace {
		if unicode.IsControl(r) {
			return kv.ErrInvalidNamespace
		}
	}
	return nil
}
