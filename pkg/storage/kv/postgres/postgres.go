// Package kvpostgres implements github.com/oarkflow/fh/pkg/storage/kv.Provider
// and kv.Store on top of PostgreSQL, using github.com/oarkflow/squealx (a
// database/sql wrapper) and its pgx-backed postgres driver.
//
// One Provider owns one backing table (named via Options.TablePrefix) shared
// by every namespace it serves; rows are distinguished by a (namespace, key)
// primary key so namespaces never observe each other's entries. Mutate uses a
// PostgreSQL advisory transaction lock keyed by (namespace, key) so
// read-modify-write callers racing across separate processes/connections are
// serialized, including the "brand-new key" case where there is no row to
// lock with a plain SELECT ... FOR UPDATE.
package kvpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/oarkflow/squealx"
	sqpostgres "github.com/oarkflow/squealx/drivers/postgres"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// defaultTablePrefix names the backing table when Options.TablePrefix is
// empty: the table is "fh_kv_entries" and its supporting index is
// "fh_kv_entries_ns_exp_idx".
const defaultTablePrefix = "fh_kv"

// tablePrefixPattern restricts Options.TablePrefix to a safe SQL identifier.
// The prefix is interpolated directly into DDL/DML text (table/index names
// cannot be bind parameters in PostgreSQL), so it must never come from
// untrusted input.
var tablePrefixPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Options configures a Provider.
type Options struct {
	// TablePrefix names the backing table, so one database can host more
	// than one Provider instance without collision. Defaults to "fh_kv",
	// producing table "fh_kv_entries" and index "fh_kv_entries_ns_exp_idx".
	// Must be a valid, unquoted SQL identifier (letters, digits, underscore,
	// not starting with a digit) — it is never a user-controlled value.
	TablePrefix string

	// QueryTimeout bounds every individual Store operation (Get, Set,
	// Delete, Len, Mutate). <=0 (default) means no per-call timeout is
	// applied beyond the context passed to Provider.Store — callers relying
	// on cancellation should wrap their own deadline around Provider.Store
	// and set QueryTimeout for defense in depth on the Store methods, which
	// (per the kv.Store contract) do not accept a context themselves.
	QueryTimeout time.Duration

	// The following pool settings are applied only by NewProviderFromDSN,
	// which opens and therefore owns its own connection. NewProvider never
	// mutates a caller-supplied *squealx.DB's pool configuration.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

func (o Options) withDefaults() Options {
	if strings.TrimSpace(o.TablePrefix) == "" {
		o.TablePrefix = defaultTablePrefix
	}
	if o.MaxOpenConns <= 0 {
		o.MaxOpenConns = 10
	}
	if o.MaxIdleConns <= 0 {
		o.MaxIdleConns = 5
	}
	if o.ConnMaxLifetime <= 0 {
		o.ConnMaxLifetime = 30 * time.Minute
	}
	if o.ConnMaxIdleTime <= 0 {
		o.ConnMaxIdleTime = 5 * time.Minute
	}
	return o
}

func validateTablePrefix(p string) error {
	if len(p) == 0 || len(p) > 40 || !tablePrefixPattern.MatchString(p) {
		return fmt.Errorf("kvpostgres: invalid table prefix %q", p)
	}
	return nil
}

// Provider is a PostgreSQL-backed kv.Provider. Construct one with NewProvider
// (caller-owned connection) or NewProviderFromDSN (Provider-owned
// connection); see each constructor's doc for Close semantics.
type Provider struct {
	db           *squealx.DB
	ownsDB       bool
	table        string
	queryTimeout time.Duration

	mu     sync.Mutex
	stores map[string]*Store
	closed bool
}

// NewProvider adapts an already-opened, caller-owned *squealx.DB into a
// Provider. Provider.Close does NOT close db: the caller remains responsible
// for its lifecycle and may keep using or sharing it (including with other
// Providers using a different TablePrefix) after this Provider is closed.
//
// NewProvider creates the backing table and supporting index if they do not
// already exist.
func NewProvider(ctx context.Context, db *squealx.DB, opts Options) (*Provider, error) {
	if db == nil {
		return nil, errors.New("kvpostgres: nil db")
	}
	return newProvider(ctx, db, false, opts)
}

// NewProviderFromDSN opens its own PostgreSQL connection via
// github.com/oarkflow/squealx/drivers/postgres (pgx's stdlib driver) and
// returns a Provider that owns it: Provider.Close closes the connection. id
// is squealx's connection identifier (used for diagnostics/metrics; any
// caller-meaningful string is fine, e.g. a service or shard name).
//
// NewProviderFromDSN creates the backing table and supporting index if they
// do not already exist, and applies Options' pool-sizing fields to the
// connection it opens.
func NewProviderFromDSN(ctx context.Context, dsn string, id string, opts Options) (*Provider, error) {
	db, err := sqpostgres.Open(dsn, id)
	if err != nil {
		return nil, fmt.Errorf("kvpostgres: open: %w", err)
	}
	o := opts.withDefaults()
	db.SetMaxOpenConns(o.MaxOpenConns)
	db.SetMaxIdleConns(o.MaxIdleConns)
	db.SetConnMaxLifetime(o.ConnMaxLifetime)
	db.SetConnMaxIdleTime(o.ConnMaxIdleTime)

	p, err := newProvider(ctx, db, true, opts)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return p, nil
}

func newProvider(ctx context.Context, db *squealx.DB, ownsDB bool, opts Options) (*Provider, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	o := opts.withDefaults()
	if err := validateTablePrefix(o.TablePrefix); err != nil {
		return nil, err
	}
	p := &Provider{
		db:           db,
		ownsDB:       ownsDB,
		table:        o.TablePrefix + "_entries",
		queryTimeout: o.QueryTimeout,
		stores:       make(map[string]*Store),
	}
	if err := p.ensureSchema(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Provider) ensureSchema(ctx context.Context) error {
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		namespace TEXT NOT NULL,
		key TEXT NOT NULL,
		value BYTEA,
		expires_at TIMESTAMPTZ,
		PRIMARY KEY (namespace, key)
	)`, p.table)
	if _, err := p.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("kvpostgres: create table %s: %w", p.table, err)
	}
	idx := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (namespace, expires_at)`, p.table+"_ns_exp_idx", p.table)
	if _, err := p.db.ExecContext(ctx, idx); err != nil {
		return fmt.Errorf("kvpostgres: create index on %s: %w", p.table, err)
	}
	return nil
}

// Store returns the isolated namespaced store, opening it on first use.
// Repeated calls for the same namespace return the same handle.
func (p *Provider) Store(ctx context.Context, namespace string) (kv.Store, error) {
	if err := validateProviderRequest(ctx, namespace); err != nil {
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
	s := &Store{provider: p, namespace: namespace}
	p.stores[namespace] = s
	return s, nil
}

// Close closes every namespace handle this Provider issued. It closes the
// underlying *squealx.DB only if this Provider opened it (NewProviderFromDSN);
// a caller-supplied connection (NewProvider) is left open for the caller.
// Close is idempotent and safe to call more than once.
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
	if p.ownsDB {
		return p.db.Close()
	}
	return nil
}

// validateProviderRequest mirrors the validation kv's built-in providers
// (MemoryProvider, FileProvider) apply to namespace requests: Provider is a
// public interface and must not trust caller input.
func validateProviderRequest(ctx context.Context, namespace string) error {
	if ctx == nil {
		return errors.New("kvpostgres: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if namespace == "" || len(namespace) > 128 || !utf8.ValidString(namespace) || strings.TrimSpace(namespace) != namespace {
		return kv.ErrInvalidNamespace
	}
	for _, r := range namespace {
		if unicode.IsControl(r) {
			return kv.ErrInvalidNamespace
		}
	}
	return nil
}

// Store is the PostgreSQL-backed kv.Store for one namespace. All rows it
// reads and writes carry that namespace in their primary key, so distinct
// Store handles from the same Provider never observe each other's keys.
//
// Store's methods use context.Background() (optionally bounded by
// Options.QueryTimeout) because the kv.Store interface does not accept a
// context per call.
type Store struct {
	provider  *Provider
	namespace string
	closed    atomic.Bool
}

var _ kv.Store = (*Store)(nil)
var _ kv.Provider = (*Provider)(nil)

func (s *Store) checkOpen() error {
	if s.closed.Load() {
		return kv.ErrClosed
	}
	return nil
}

func (s *Store) ctx() (context.Context, context.CancelFunc) {
	if s.provider.queryTimeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), s.provider.queryTimeout)
}

// Close marks this namespace handle closed; subsequent calls on it return
// kv.ErrClosed. It does not touch the Provider's shared connection or any
// sibling Store returned for a different namespace — only Provider.Close
// closes the shared *squealx.DB, and only when the Provider opened it.
// Close is idempotent.
func (s *Store) Close() error {
	s.closed.Store(true)
	return nil
}

// Get returns the value for key and true if present and not expired. A
// lazily-discovered expired row is opportunistically (best-effort) deleted.
func (s *Store) Get(key string) ([]byte, bool, error) {
	if err := s.checkOpen(); err != nil {
		return nil, false, err
	}
	ctx, cancel := s.ctx()
	defer cancel()

	query := fmt.Sprintf(`SELECT value, expires_at FROM %s WHERE namespace = $1 AND key = $2`, s.provider.table)
	var value []byte
	var expiresAt sql.NullTime
	err := s.provider.db.QueryRowContext(ctx, query, s.namespace, key).Scan(&value, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if expiresAt.Valid && !expiresAt.Time.After(time.Now()) {
		s.deleteBestEffort(key)
		return nil, false, nil
	}
	return value, true, nil
}

func (s *Store) deleteBestEffort(key string) {
	ctx, cancel := s.ctx()
	defer cancel()
	query := fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1 AND key = $2`, s.provider.table)
	_, _ = s.provider.db.ExecContext(ctx, query, s.namespace, key)
}

// Set stores value for key with the given ttl (<=0 means no expiry),
// replacing any previous value and expiry.
func (s *Store) Set(key string, value []byte, ttl time.Duration) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	ctx, cancel := s.ctx()
	defer cancel()

	expiresAt := ttlToNullTime(ttl)
	query := fmt.Sprintf(`INSERT INTO %s (namespace, key, value, expires_at) VALUES ($1, $2, $3, $4)
		ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at`, s.provider.table)
	_, err := s.provider.db.ExecContext(ctx, query, s.namespace, key, value, expiresAt)
	return err
}

// Delete removes key. Deleting a missing key is not an error.
func (s *Store) Delete(key string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	ctx, cancel := s.ctx()
	defer cancel()

	query := fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1 AND key = $2`, s.provider.table)
	_, err := s.provider.db.ExecContext(ctx, query, s.namespace, key)
	return err
}

// Len prunes expired rows for this namespace and reports the number of
// remaining (live) entries.
func (s *Store) Len() (int, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	ctx, cancel := s.ctx()
	defer cancel()

	pruneQuery := fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1 AND expires_at IS NOT NULL AND expires_at <= now()`, s.provider.table)
	if _, err := s.provider.db.ExecContext(ctx, pruneQuery, s.namespace); err != nil {
		return 0, err
	}
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE namespace = $1`, s.provider.table)
	var n int
	if err := s.provider.db.QueryRowContext(ctx, countQuery, s.namespace).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// Mutate atomically loads the current value for key, calls fn, and writes
// back fn's decision — all inside one PostgreSQL transaction guarded by a
// pg_advisory_xact_lock keyed by (namespace, key).
//
// The advisory lock, not a SELECT ... FOR UPDATE on the row, is what makes
// this safe when the key does not exist yet: a row lock has nothing to lock
// until a row exists, so two concurrent first-writers could otherwise both
// decide "absent" and race on the ON CONFLICT upsert. Taking the advisory
// lock unconditionally, before any SELECT, serializes every Mutate caller —
// across every process sharing this database — for that exact key, whether
// or not a row exists yet. The lock is scoped to the transaction and
// releases automatically on commit or rollback.
func (s *Store) Mutate(key string, fn func(current []byte, exists bool) (next []byte, ttl time.Duration, ok bool, err error)) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	ctx, cancel := s.ctx()
	defer cancel()

	table := s.provider.table
	namespace := s.namespace

	return s.provider.db.WithTx(ctx, nil, func(tx squealx.SQLTx) error {
		// hashtextextended(text, seed bigint) -> bigint is a stable 64-bit
		// hash usable directly as pg_advisory_xact_lock's key. namespace and
		// key are hashed separately (with distinct seeds, then XORed) rather
		// than concatenated in Go: namespaces and keys are arbitrary strings
		// that may contain any byte a caller chooses — including NUL, which
		// PostgreSQL text parameters reject outright — so building one
		// composite string ourselves is both unsafe and unnecessary. Hashing
		// each argument independently in SQL sidesteps that entirely.
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0) # hashtextextended($2, 1))`, namespace, key); err != nil {
			return fmt.Errorf("kvpostgres: acquire advisory lock: %w", err)
		}

		selectQuery := fmt.Sprintf(`SELECT value, expires_at FROM %s WHERE namespace = $1 AND key = $2`, table)
		var current []byte
		var expiresAt sql.NullTime
		var exists bool
		switch err := tx.QueryRowContext(ctx, selectQuery, namespace, key).Scan(&current, &expiresAt); {
		case err == nil:
			exists = true
		case errors.Is(err, sql.ErrNoRows):
			exists = false
		default:
			return err
		}

		if exists && expiresAt.Valid && !expiresAt.Time.After(time.Now()) {
			// Lazily-expired row: treat as absent and remove it here, inside
			// the same lock-protected transaction, so the upsert below never
			// collides with a stale expired row.
			current, exists = nil, false
			delQuery := fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1 AND key = $2`, table)
			if _, err := tx.ExecContext(ctx, delQuery, namespace, key); err != nil {
				return err
			}
		}

		next, ttl, ok, err := fn(current, exists)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		upsertQuery := fmt.Sprintf(`INSERT INTO %s (namespace, key, value, expires_at) VALUES ($1, $2, $3, $4)
			ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at`, table)
		_, err = tx.ExecContext(ctx, upsertQuery, namespace, key, next, ttlToNullTime(ttl))
		return err
	})
}

func ttlToNullTime(ttl time.Duration) sql.NullTime {
	if ttl <= 0 {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: time.Now().Add(ttl), Valid: true}
}
