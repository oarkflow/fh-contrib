package kvpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqpostgres "github.com/oarkflow/squealx/drivers/postgres"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// testDSN points at the already-running PostgreSQL instance this suite is
// meant to exercise. Nothing in this package starts or stops that server.
const testDSN = "host=localhost port=15432 user=postgres password=fhtest dbname=fhkvtest sslmode=disable"

var prefixCounter atomic.Int64

// uniquePrefix gives every test its own backing table so tests never observe
// each other's rows, even when Len() counts every row in the table.
func uniquePrefix(t *testing.T) string {
	t.Helper()
	n := prefixCounter.Add(1)
	return fmt.Sprintf("fhkvtest_%d_%d", time.Now().UnixNano(), n)
}

// newTestProvider opens an owned connection (NewProviderFromDSN) against a
// fresh table and arranges for both the table and the connection to be
// cleaned up when the test ends.
func newTestProvider(t *testing.T, opts Options) *Provider {
	t.Helper()
	ctx := context.Background()
	if opts.TablePrefix == "" {
		opts.TablePrefix = uniquePrefix(t)
	}
	p, err := NewProviderFromDSN(ctx, testDSN, "test-"+opts.TablePrefix, opts)
	if err != nil {
		t.Fatalf("NewProviderFromDSN: %v", err)
	}
	table := p.table
	t.Cleanup(func() {
		dropTestTable(t, table)
		_ = p.Close()
	})
	return p
}

func dropTestTable(t *testing.T, table string) {
	t.Helper()
	db, err := sqpostgres.Open(testDSN, "drop-"+table)
	if err != nil {
		t.Logf("dropTestTable: open: %v", err)
		return
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)); err != nil {
		t.Logf("dropTestTable: drop %s: %v", table, err)
	}
}

// rawGet reads a row directly, bypassing Store, so tests can assert that a
// no-op or failed Mutate left both the value and the expiry column exactly
// as they were.
func (p *Provider) rawGet(ctx context.Context, namespace, key string) (value []byte, expiresAt sql.NullTime, found bool, err error) {
	query := fmt.Sprintf(`SELECT value, expires_at FROM %s WHERE namespace = $1 AND key = $2`, p.table)
	err = p.db.QueryRowContext(ctx, query, namespace, key).Scan(&value, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.NullTime{}, false, nil
		}
		return nil, sql.NullTime{}, false, err
	}
	return value, expiresAt, true, nil
}

func TestGetSetDeleteAndTTLExpiry(t *testing.T) {
	p := newTestProvider(t, Options{})
	ctx := context.Background()
	store, err := p.Store(ctx, "ns/basic")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if _, ok, err := store.Get("missing"); err != nil || ok {
		t.Fatalf("expected miss for absent key, got ok=%v err=%v", ok, err)
	}

	if err := store.Set("k1", []byte("v1"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, ok, err := store.Get("k1")
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("Get after Set: v=%q ok=%v err=%v", v, ok, err)
	}

	// Overwrite.
	if err := store.Set("k1", []byte("v2"), 0); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	v, ok, err = store.Get("k1")
	if err != nil || !ok || string(v) != "v2" {
		t.Fatalf("Get after overwrite: v=%q ok=%v err=%v", v, ok, err)
	}

	if err := store.Delete("k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := store.Get("k1"); err != nil || ok {
		t.Fatalf("expected miss after Delete, got ok=%v err=%v", ok, err)
	}

	// Deleting an already-missing key is not an error.
	if err := store.Delete("never-existed"); err != nil {
		t.Fatalf("Delete of missing key returned error: %v", err)
	}

	// TTL expiry: poll until expired rather than a single fixed sleep.
	if err := store.Set("ttl-key", []byte("v"), 60*time.Millisecond); err != nil {
		t.Fatalf("Set with ttl: %v", err)
	}
	if _, ok, err := store.Get("ttl-key"); err != nil || !ok {
		t.Fatalf("expected ttl-key present immediately after Set: ok=%v err=%v", ok, err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, ok, err := store.Get("ttl-key")
		if err != nil {
			t.Fatalf("Get during expiry poll: %v", err)
		}
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ttl-key did not expire within deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A zero/negative ttl never expires on its own.
	if err := store.Set("no-ttl", []byte("v"), 0); err != nil {
		t.Fatalf("Set no-ttl: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok, err := store.Get("no-ttl"); err != nil || !ok {
		t.Fatalf("no-ttl key should never expire on its own: ok=%v err=%v", ok, err)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	p := newTestProvider(t, Options{})
	ctx := context.Background()
	a, err := p.Store(ctx, "ns/a")
	if err != nil {
		t.Fatalf("Store a: %v", err)
	}
	b, err := p.Store(ctx, "ns/b")
	if err != nil {
		t.Fatalf("Store b: %v", err)
	}

	if err := a.Set("shared-key", []byte("from-a"), 0); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if _, ok, err := b.Get("shared-key"); err != nil || ok {
		t.Fatalf("namespace b must not see namespace a's key: ok=%v err=%v", ok, err)
	}

	if err := b.Set("shared-key", []byte("from-b"), 0); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	v, ok, err := a.Get("shared-key")
	if err != nil || !ok || string(v) != "from-a" {
		t.Fatalf("namespace a's value must be independent of b's write: v=%q ok=%v err=%v", v, ok, err)
	}

	for i := 0; i < 3; i++ {
		if err := b.Set(fmt.Sprintf("extra-%d", i), []byte("x"), 0); err != nil {
			t.Fatalf("Set extra-%d: %v", i, err)
		}
	}

	na, err := a.Len()
	if err != nil {
		t.Fatalf("Len a: %v", err)
	}
	nb, err := b.Len()
	if err != nil {
		t.Fatalf("Len b: %v", err)
	}
	if na != 1 {
		t.Fatalf("expected namespace a Len()=1, got %d", na)
	}
	if nb != 4 {
		t.Fatalf("expected namespace b Len()=4, got %d", nb)
	}

	// Repeated Provider.Store calls for the same namespace return the same
	// logical store (same underlying rows).
	a2, err := p.Store(ctx, "ns/a")
	if err != nil {
		t.Fatalf("Store a again: %v", err)
	}
	if v, ok, err := a2.Get("shared-key"); err != nil || !ok || string(v) != "from-a" {
		t.Fatalf("second Store() handle for ns/a diverged: v=%q ok=%v err=%v", v, ok, err)
	}
}

// maxConcurrentTestConnections caps how many real PostgreSQL connections the
// concurrency tests open at once. The shared test instance's
// max_connections is finite (and shared with any other session on it), so
// this stays comfortably under that cap regardless of how many goroutines
// (`workers`) issue Mutate calls — goroutines are distributed round-robin
// across this many separate, real *squealx.DB connections/processes-in-spirit,
// each limited to exactly one physical connection (Options.MaxOpenConns: 1),
// so the total connection footprint is deterministic.
const maxConcurrentTestConnections = 20

// runConcurrentIncrements opens maxConcurrentTestConnections separate
// *squealx.DB connections (each wrapped in its own Provider against the same
// table, simulating separate processes sharing one database) and distributes
// `workers` concurrent Mutate calls across them round-robin, each Mutate
// call incrementing an integer counter (encoded as decimal text) stored at
// the same (namespace, key). Using more goroutines than real connections
// still exercises the cross-connection race — Go's pool simply queues extra
// goroutines for that connection's single slot — while keeping this test
// suite's total connection usage independent of how many goroutines it runs.
func runConcurrentIncrements(t *testing.T, prefix, namespace, key string, workers int) {
	t.Helper()
	ctx := context.Background()

	numConns := maxConcurrentTestConnections
	if workers < numConns {
		numConns = workers
	}
	providers := make([]*Provider, numConns)
	for i := 0; i < numConns; i++ {
		id := fmt.Sprintf("worker-%s-%d-%d", prefix, time.Now().UnixNano(), i)
		p, err := NewProviderFromDSN(ctx, testDSN, id, Options{TablePrefix: prefix, MaxOpenConns: 1})
		if err != nil {
			t.Fatalf("connection %d: NewProviderFromDSN: %v", i, err)
		}
		providers[i] = p
	}
	defer func() {
		for _, p := range providers {
			_ = p.Close()
		}
	}()

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(p *Provider) {
			defer wg.Done()
			s, err := p.Store(ctx, namespace)
			if err != nil {
				errs <- fmt.Errorf("Store: %w", err)
				return
			}
			err = s.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
				n := 0
				if exists {
					parsed, perr := strconv.Atoi(string(current))
					if perr != nil {
						return nil, 0, false, perr
					}
					n = parsed
				}
				n++
				return []byte(strconv.Itoa(n)), 0, true, nil
			})
			if err != nil {
				errs <- fmt.Errorf("Mutate: %w", err)
			}
		}(providers[i%numConns])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent increment worker failed: %v", err)
	}
}

// TestMutateConcurrentIncrementSteadyState proves Mutate serializes
// concurrent read-modify-write callers across separate connections/processes
// for a key that already exists: no update is lost.
func TestMutateConcurrentIncrementSteadyState(t *testing.T) {
	prefix := uniquePrefix(t)
	p := newTestProvider(t, Options{TablePrefix: prefix})
	ctx := context.Background()
	namespace := "concurrency/steady"

	store, err := p.Store(ctx, namespace)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := store.Set("counter", []byte("0"), 0); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	const workers = 100
	runConcurrentIncrements(t, prefix, namespace, "counter", workers)

	v, ok, err := store.Get("counter")
	if err != nil {
		t.Fatalf("Get after concurrent increments: %v", err)
	}
	if !ok {
		t.Fatalf("counter missing after concurrent increments")
	}
	got, err := strconv.Atoi(string(v))
	if err != nil {
		t.Fatalf("counter value %q is not an integer: %v", v, err)
	}
	if got != workers {
		t.Fatalf("lost updates: expected counter == %d, got %d", workers, got)
	}
}

// TestMutateConcurrentIncrementBrandNewKey is the case a naive
// `SELECT ... FOR UPDATE` design gets wrong: nothing exists yet, so there is
// no row to lock, and many first-writers could otherwise all read
// exists=false and race on the very first insert. The advisory
// transaction lock must still serialize them.
func TestMutateConcurrentIncrementBrandNewKey(t *testing.T) {
	prefix := uniquePrefix(t)
	p := newTestProvider(t, Options{TablePrefix: prefix})
	ctx := context.Background()
	namespace := "concurrency/newkey"

	store, err := p.Store(ctx, namespace)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	// Deliberately do not Set an initial value: "counter" does not exist in
	// this namespace yet, and every worker below races to create it.

	const workers = 100
	runConcurrentIncrements(t, prefix, namespace, "counter", workers)

	v, ok, err := store.Get("counter")
	if err != nil {
		t.Fatalf("Get after concurrent first-write race: %v", err)
	}
	if !ok {
		t.Fatalf("counter missing after concurrent first-write race")
	}
	got, err := strconv.Atoi(string(v))
	if err != nil {
		t.Fatalf("counter value %q is not an integer: %v", v, err)
	}
	if got != workers {
		t.Fatalf("first-write race lost updates: expected counter == %d, got %d", workers, got)
	}
}

func TestMutateNoOpLeavesEntryUntouched(t *testing.T) {
	p := newTestProvider(t, Options{})
	ctx := context.Background()
	store, err := p.Store(ctx, "ns/noop")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := store.Set("k", []byte("original"), time.Hour); err != nil {
		t.Fatalf("Set: %v", err)
	}
	beforeValue, beforeExpiry, found, err := p.rawGet(ctx, "ns/noop", "k")
	if err != nil || !found {
		t.Fatalf("expected row present before Mutate: found=%v err=%v", found, err)
	}

	err = store.Mutate("k", func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists || string(current) != "original" {
			t.Fatalf("fn saw unexpected state: current=%q exists=%v", current, exists)
		}
		return []byte("should-never-be-written"), time.Minute, false, nil
	})
	if err != nil {
		t.Fatalf("Mutate returned error for ok=false: %v", err)
	}

	v, ok, err := store.Get("k")
	if err != nil || !ok || string(v) != "original" {
		t.Fatalf("Get after ok=false Mutate: v=%q ok=%v err=%v", v, ok, err)
	}

	afterValue, afterExpiry, found, err := p.rawGet(ctx, "ns/noop", "k")
	if err != nil || !found {
		t.Fatalf("expected row still present after ok=false Mutate: found=%v err=%v", found, err)
	}
	if string(afterValue) != string(beforeValue) {
		t.Fatalf("value changed despite ok=false: before=%q after=%q", beforeValue, afterValue)
	}
	if beforeExpiry.Valid != afterExpiry.Valid || !beforeExpiry.Time.Equal(afterExpiry.Time) {
		t.Fatalf("ttl changed despite ok=false: before=%v after=%v", beforeExpiry, afterExpiry)
	}
}

func TestMutateFnErrorLeavesEntryUntouchedAndPropagates(t *testing.T) {
	p := newTestProvider(t, Options{})
	ctx := context.Background()
	store, err := p.Store(ctx, "ns/fnerr")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := store.Set("k", []byte("original"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	beforeValue, _, found, err := p.rawGet(ctx, "ns/fnerr", "k")
	if err != nil || !found {
		t.Fatalf("expected row present before Mutate: found=%v err=%v", found, err)
	}

	sentinel := errors.New("boom")
	err = store.Mutate("k", func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		return []byte("should-never-be-written"), 0, true, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected Mutate to propagate the fn error, got %v", err)
	}

	v, ok, err := store.Get("k")
	if err != nil || !ok || string(v) != "original" {
		t.Fatalf("entry changed despite fn error: v=%q ok=%v err=%v", v, ok, err)
	}
	afterValue, _, found, err := p.rawGet(ctx, "ns/fnerr", "k")
	if err != nil || !found {
		t.Fatalf("expected row still present after fn error: found=%v err=%v", found, err)
	}
	if string(afterValue) != string(beforeValue) {
		t.Fatalf("value changed despite fn error: before=%q after=%q", beforeValue, afterValue)
	}
}

func TestStoreCloseOnlyAffectsThatHandle(t *testing.T) {
	p := newTestProvider(t, Options{})
	ctx := context.Background()
	s1, err := p.Store(ctx, "ns/one")
	if err != nil {
		t.Fatalf("Store s1: %v", err)
	}
	s2, err := p.Store(ctx, "ns/two")
	if err != nil {
		t.Fatalf("Store s2: %v", err)
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("s1.Close: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("s1.Close should be idempotent: %v", err)
	}

	if _, _, err := s1.Get("k"); !errors.Is(err, kv.ErrClosed) {
		t.Fatalf("expected ErrClosed from closed store, got %v", err)
	}
	if err := s1.Set("k", []byte("v"), 0); !errors.Is(err, kv.ErrClosed) {
		t.Fatalf("expected ErrClosed from closed store Set, got %v", err)
	}

	// The sibling store, and the shared connection/provider, are unaffected.
	if err := s2.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("sibling store affected by unrelated Close: %v", err)
	}
	if v, ok, err := s2.Get("k"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("sibling store broken after unrelated Close: v=%q ok=%v err=%v", v, ok, err)
	}
}

func TestStoreRejectsInvalidNamespace(t *testing.T) {
	p := newTestProvider(t, Options{})
	ctx := context.Background()

	if _, err := p.Store(ctx, ""); !errors.Is(err, kv.ErrInvalidNamespace) {
		t.Fatalf("expected ErrInvalidNamespace for empty namespace, got %v", err)
	}
	if _, err := p.Store(ctx, "  padded  "); !errors.Is(err, kv.ErrInvalidNamespace) {
		t.Fatalf("expected ErrInvalidNamespace for padded namespace, got %v", err)
	}
	if _, err := p.Store(ctx, "ns/with\x00control"); !errors.Is(err, kv.ErrInvalidNamespace) {
		t.Fatalf("expected ErrInvalidNamespace for control characters, got %v", err)
	}
}

// TestProviderCloseCallerOwnedConnectionSurvives asserts NewProvider's
// Close semantics: the caller-supplied *squealx.DB must remain usable after
// Provider.Close, since the caller — not the Provider — owns it.
func TestProviderCloseCallerOwnedConnectionSurvives(t *testing.T) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	db, err := sqpostgres.Open(testDSN, "caller-owned-"+prefix)
	if err != nil {
		t.Fatalf("sqpostgres.Open: %v", err)
	}
	defer db.Close()

	p, err := NewProvider(ctx, db, Options{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	table := p.table
	t.Cleanup(func() { dropTestTable(t, table) })

	store, err := p.Store(ctx, "ns/owner")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Provider.Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Provider.Close should be idempotent: %v", err)
	}

	// The caller-supplied connection must still work.
	if err := db.Ping(); err != nil {
		t.Fatalf("caller-owned db should survive Provider.Close, ping failed: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf(`SELECT 1 FROM %s LIMIT 1`, table)); err != nil {
		t.Fatalf("caller-owned db unusable after Provider.Close: %v", err)
	}

	// But the store handle the Provider issued is now closed.
	if _, _, err := store.Get("k"); !errors.Is(err, kv.ErrClosed) {
		t.Fatalf("expected ErrClosed from a store after Provider.Close, got %v", err)
	}
}

// TestProviderCloseOwnedConnectionActuallyCloses asserts NewProviderFromDSN's
// Close semantics: the connection the Provider opened for itself must
// actually close.
func TestProviderCloseOwnedConnectionActuallyCloses(t *testing.T) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	p, err := NewProviderFromDSN(ctx, testDSN, "owned-"+prefix, Options{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("NewProviderFromDSN: %v", err)
	}
	table := p.table
	t.Cleanup(func() { dropTestTable(t, table) })

	store, err := p.Store(ctx, "ns/owned")
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Provider.Close: %v", err)
	}

	if err := p.db.Ping(); err == nil {
		t.Fatalf("expected the owned connection to be closed after Provider.Close")
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Provider.Close should be idempotent: %v", err)
	}
}
