package kvredis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

const testRedisAddr = "localhost:16379"

// newTestClient returns a fresh client pointed at the live test Redis
// instance, along with a cleanup func that closes it.
func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v (is redis running at %s?)", err, testRedisAddr)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// testNamespace returns a unique namespace per test so parallel/concurrent
// tests never collide on the same Redis keys, and cleans up every key it
// created (data + index) when the test ends.
func testNamespace(t *testing.T, client *redis.Client, prefix string) string {
	t.Helper()
	ns := fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		ctx := context.Background()
		iter := client.Scan(ctx, 0, prefix+ns+"*", 100).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			_ = client.Del(ctx, keys...).Err()
		}
	})
	return ns
}

func pollUntil(t *testing.T, deadline time.Duration, interval time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		if cond() {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("condition not met within %s", deadline)
		}
		time.Sleep(interval)
	}
}

func TestGetSetDeleteRoundTrip(t *testing.T) {
	client := newTestClient(t)
	ns := testNamespace(t, client, defaultPrefix)
	p := New(client)
	t.Cleanup(func() { _ = p.Close() })

	store, err := p.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if _, ok, err := store.Get("missing"); err != nil || ok {
		t.Fatalf("Get missing = ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	if err := store.Set("k1", []byte("v1"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, ok, err := store.Get("k1")
	if err != nil || !ok || string(val) != "v1" {
		t.Fatalf("Get k1 = %q ok=%v err=%v, want v1/true/nil", val, ok, err)
	}

	if err := store.Delete("k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := store.Get("k1"); err != nil || ok {
		t.Fatalf("Get after Delete = ok=%v err=%v, want false/nil", ok, err)
	}

	// Deleting a missing key is not an error.
	if err := store.Delete("does-not-exist"); err != nil {
		t.Fatalf("Delete missing key: %v", err)
	}
}

func TestGetSetTTLExpiry(t *testing.T) {
	client := newTestClient(t)
	ns := testNamespace(t, client, defaultPrefix)
	p := New(client)
	t.Cleanup(func() { _ = p.Close() })

	store, err := p.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := store.Set("ttl-key", []byte("v"), 50*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Immediately visible.
	if _, ok, err := store.Get("ttl-key"); err != nil || !ok {
		t.Fatalf("Get right after Set = ok=%v err=%v, want true/nil", ok, err)
	}

	pollUntil(t, 5*time.Second, 20*time.Millisecond, func() bool {
		_, ok, err := store.Get("ttl-key")
		return err == nil && !ok
	})
}

func TestLenTracksLiveEntriesAndPrunesExpired(t *testing.T) {
	client := newTestClient(t)
	ns := testNamespace(t, client, defaultPrefix)
	p := New(client)
	t.Cleanup(func() { _ = p.Close() })

	store, err := p.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if n, err := store.Len(); err != nil || n != 0 {
		t.Fatalf("Len empty = %d, err=%v, want 0/nil", n, err)
	}

	if err := store.Set("a", []byte("1"), 0); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := store.Set("b", []byte("2"), 0); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	if err := store.Set("c", []byte("3"), 2*time.Second); err != nil {
		t.Fatalf("Set c: %v", err)
	}

	if n, err := store.Len(); err != nil || n != 3 {
		t.Fatalf("Len after 3 sets = %d, err=%v, want 3/nil", n, err)
	}

	pollUntil(t, 5*time.Second, 20*time.Millisecond, func() bool {
		n, err := store.Len()
		return err == nil && n == 2
	})

	if err := store.Delete("a"); err != nil {
		t.Fatalf("Delete a: %v", err)
	}
	if n, err := store.Len(); err != nil || n != 1 {
		t.Fatalf("Len after Delete a = %d, err=%v, want 1/nil", n, err)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	client := newTestClient(t)
	nsA := testNamespace(t, client, defaultPrefix)
	nsB := testNamespace(t, client, defaultPrefix)
	p := New(client)
	t.Cleanup(func() { _ = p.Close() })

	storeA, err := p.Store(context.Background(), nsA)
	if err != nil {
		t.Fatalf("Store A: %v", err)
	}
	storeB, err := p.Store(context.Background(), nsB)
	if err != nil {
		t.Fatalf("Store B: %v", err)
	}

	if err := storeA.Set("shared-key", []byte("from-a"), 0); err != nil {
		t.Fatalf("Set A: %v", err)
	}
	if err := storeB.Set("shared-key", []byte("from-b"), 0); err != nil {
		t.Fatalf("Set B: %v", err)
	}
	if err := storeA.Set("only-in-a", []byte("x"), 0); err != nil {
		t.Fatalf("Set A only: %v", err)
	}

	valA, ok, err := storeA.Get("shared-key")
	if err != nil || !ok || string(valA) != "from-a" {
		t.Fatalf("storeA.Get(shared-key) = %q ok=%v err=%v, want from-a/true/nil", valA, ok, err)
	}
	valB, ok, err := storeB.Get("shared-key")
	if err != nil || !ok || string(valB) != "from-b" {
		t.Fatalf("storeB.Get(shared-key) = %q ok=%v err=%v, want from-b/true/nil", valB, ok, err)
	}

	if _, ok, err := storeB.Get("only-in-a"); err != nil || ok {
		t.Fatalf("storeB.Get(only-in-a) = ok=%v err=%v, want false/nil (namespace leak)", ok, err)
	}

	nA, err := storeA.Len()
	if err != nil {
		t.Fatalf("Len A: %v", err)
	}
	nB, err := storeB.Len()
	if err != nil {
		t.Fatalf("Len B: %v", err)
	}
	if nA != 2 {
		t.Fatalf("Len A = %d, want 2", nA)
	}
	if nB != 1 {
		t.Fatalf("Len B = %d, want 1", nB)
	}

	// Repeated Store calls for the same namespace return the same logical
	// store (same underlying keys).
	storeA2, err := p.Store(context.Background(), nsA)
	if err != nil {
		t.Fatalf("Store A again: %v", err)
	}
	if val, ok, err := storeA2.Get("only-in-a"); err != nil || !ok || string(val) != "x" {
		t.Fatalf("storeA2.Get(only-in-a) = %q ok=%v err=%v, want x/true/nil", val, ok, err)
	}
}

// TestMutateAtomicityUnderConcurrency is the most important test in this
// file: many goroutines, each backed by its own *redis.Client (simulating
// separate processes sharing one Redis instance), race to increment the same
// counter key via Mutate. If the WATCH/MULTI retry loop ever lost an update,
// the final stored value would be less than the number of successful
// increments.
func TestMutateAtomicityUnderConcurrency(t *testing.T) {
	baseClient := newTestClient(t)
	ns := testNamespace(t, baseClient, defaultPrefix)

	const goroutines = 100
	const key = "counter"

	// Seed the key via one provider/store so the namespace and index exist.
	seedProvider := New(baseClient)
	t.Cleanup(func() { _ = seedProvider.Close() })
	seedStore, err := seedProvider.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("seed Store: %v", err)
	}
	if err := seedStore.Set(key, []byte("0"), 0); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	increment := func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		n := 0
		if exists && len(current) > 0 {
			parsed, err := strconv.Atoi(string(current))
			if err != nil {
				return nil, 0, false, err
			}
			n = parsed
		}
		n++
		return []byte(strconv.Itoa(n)), 0, true, nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Separate client per goroutine to simulate separate processes
			// sharing the same Redis instance.
			client := redis.NewClient(&redis.Options{Addr: testRedisAddr})
			defer client.Close()
			p := New(client)
			defer p.Close()
			store, err := p.Store(context.Background(), ns)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d Store: %w", i, err)
				return
			}
			if err := store.Mutate(key, increment); err != nil {
				errCh <- fmt.Errorf("goroutine %d Mutate: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent Mutate error: %v", err)
	}

	final, ok, err := seedStore.Get(key)
	if err != nil || !ok {
		t.Fatalf("final Get = ok=%v err=%v", ok, err)
	}
	got, err := strconv.Atoi(string(final))
	if err != nil {
		t.Fatalf("final value %q not an int: %v", final, err)
	}
	if got != goroutines {
		t.Fatalf("final counter = %d, want %d (lost update under contention)", got, goroutines)
	}
}

func TestMutateNotOkLeavesEntryUntouched(t *testing.T) {
	client := newTestClient(t)
	ns := testNamespace(t, client, defaultPrefix)
	p := New(client)
	t.Cleanup(func() { _ = p.Close() })

	store, err := p.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := store.Set("k", []byte("original"), time.Hour); err != nil {
		t.Fatalf("Set: %v", err)
	}

	called := false
	err = store.Mutate("k", func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		called = true
		if !exists || string(current) != "original" {
			t.Fatalf("Mutate fn saw current=%q exists=%v, want original/true", current, exists)
		}
		return []byte("changed"), time.Minute, false, nil
	})
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	if !called {
		t.Fatal("Mutate fn was never called")
	}

	val, ok, err := store.Get("k")
	if err != nil || !ok || string(val) != "original" {
		t.Fatalf("Get after not-ok Mutate = %q ok=%v err=%v, want original/true/nil", val, ok, err)
	}

	rawTTL, err := client.TTL(context.Background(), p.cfg.prefix+ns+":k").Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if rawTTL <= 0 || rawTTL > time.Hour {
		t.Fatalf("TTL after not-ok Mutate = %s, want roughly unchanged (~1h, still positive)", rawTTL)
	}
}

var errMutateFn = errors.New("boom: deliberate mutate failure")

func TestMutateFnErrorLeavesEntryUntouchedAndPropagates(t *testing.T) {
	client := newTestClient(t)
	ns := testNamespace(t, client, defaultPrefix)
	p := New(client)
	t.Cleanup(func() { _ = p.Close() })

	store, err := p.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := store.Set("k", []byte("original"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err = store.Mutate("k", func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		return nil, 0, false, errMutateFn
	})
	if !errors.Is(err, errMutateFn) {
		t.Fatalf("Mutate error = %v, want errors.Is(err, errMutateFn)", err)
	}

	val, ok, err := store.Get("k")
	if err != nil || !ok || string(val) != "original" {
		t.Fatalf("Get after errored Mutate = %q ok=%v err=%v, want original/true/nil", val, ok, err)
	}
}

func TestProviderCloseCallerOwnedClientSurvives(t *testing.T) {
	client := newTestClient(t)
	ns := testNamespace(t, client, defaultPrefix)
	p := New(client)

	store, err := p.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Provider.Close: %v", err)
	}

	// Store calls after Provider.Close are rejected.
	if _, err := p.Store(context.Background(), ns); !errors.Is(err, kv.ErrProviderClosed) {
		t.Fatalf("Store after Close = %v, want kv.ErrProviderClosed", err)
	}

	// The namespace Store handle obtained before Close is now closed too.
	if _, _, err := store.Get("k"); !errors.Is(err, kv.ErrClosed) {
		t.Fatalf("Get on store after Provider.Close = %v, want kv.ErrClosed", err)
	}

	// But the caller-supplied client itself is untouched: it must still work
	// for unrelated commands after the Provider that wrapped it is closed.
	if err := client.Set(context.Background(), "kvredis-survives-"+ns, "still-alive", time.Second).Err(); err != nil {
		t.Fatalf("caller-owned client unusable after Provider.Close: %v", err)
	}
	val, err := client.Get(context.Background(), "kvredis-survives-"+ns).Result()
	if err != nil || val != "still-alive" {
		t.Fatalf("caller-owned client Get after Provider.Close = %q, %v, want still-alive/nil", val, err)
	}
	_ = client.Del(context.Background(), "kvredis-survives-"+ns)
}

func TestOpenOwnsClientAndClosesIt(t *testing.T) {
	p, err := Open(&redis.Options{Addr: testRedisAddr})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ns := testNamespace(t, p.client, defaultPrefix)

	store, err := p.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Provider.Close: %v", err)
	}

	// The owned client is now closed: any command on it should fail.
	if err := p.client.Ping(context.Background()).Err(); err == nil {
		t.Fatal("expected owned client to be closed after Provider.Close, but Ping succeeded")
	}
}

func TestStoreCloseOnlyAffectsItself(t *testing.T) {
	client := newTestClient(t)
	nsA := testNamespace(t, client, defaultPrefix)
	nsB := testNamespace(t, client, defaultPrefix)
	p := New(client)
	t.Cleanup(func() { _ = p.Close() })

	storeA, err := p.Store(context.Background(), nsA)
	if err != nil {
		t.Fatalf("Store A: %v", err)
	}
	storeB, err := p.Store(context.Background(), nsB)
	if err != nil {
		t.Fatalf("Store B: %v", err)
	}
	if err := storeB.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("Set B: %v", err)
	}

	if err := storeA.Close(); err != nil {
		t.Fatalf("Close A: %v", err)
	}

	if _, _, err := storeA.Get("k"); !errors.Is(err, kv.ErrClosed) {
		t.Fatalf("Get on closed store A = %v, want kv.ErrClosed", err)
	}

	// Sibling store B is unaffected.
	if val, ok, err := storeB.Get("k"); err != nil || !ok || string(val) != "v" {
		t.Fatalf("Get on sibling store B = %q ok=%v err=%v, want v/true/nil", val, ok, err)
	}

	// The shared client still works directly.
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("shared client Ping after Store.Close: %v", err)
	}

	// Close is idempotent.
	if err := storeA.Close(); err != nil {
		t.Fatalf("second Close A: %v", err)
	}
}

func TestInvalidNamespaceRejected(t *testing.T) {
	client := newTestClient(t)
	p := New(client)
	t.Cleanup(func() { _ = p.Close() })

	cases := []string{"", " leading-space", "trailing-space ", string([]byte{0x00}) + "ctrl"}
	for _, ns := range cases {
		if _, err := p.Store(context.Background(), ns); !errors.Is(err, kv.ErrInvalidNamespace) {
			t.Fatalf("Store(%q) = %v, want kv.ErrInvalidNamespace", ns, err)
		}
	}
}

func TestWithPrefixOption(t *testing.T) {
	client := newTestClient(t)
	ns := testNamespace(t, client, "custom:")
	p := New(client, WithPrefix("custom:"))
	t.Cleanup(func() { _ = p.Close() })

	store, err := p.Store(context.Background(), ns)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := store.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	raw, err := client.Get(context.Background(), "custom:"+ns+":k").Result()
	if err != nil || raw != "v" {
		t.Fatalf("raw Get on custom-prefixed key = %q, %v, want v/nil", raw, err)
	}
}
