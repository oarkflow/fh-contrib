# kvredis

Redis-backed `kv.Provider`/`kv.Store` for `github.com/oarkflow/fh/pkg/storage/kv`,
built on `github.com/redis/go-redis/v9`.

## What it does

Each namespace `Store` from a `Provider` keeps its entries under
`<prefix><namespace>:<key>` (`prefix` defaults to `"fh:"`), relying on Redis's
own key TTL for expiry. Because Redis has no O(1) "count keys under a prefix"
operation, each namespace also keeps a sorted set,
`<prefix><namespace>:__index`, mapping key -> absolute expiry, so `Len` can
answer without scanning the keyspace; `Len` prunes crossed-over-expired
members before returning the count, so it may lag slightly behind Redis's own
lazy/active expiry (this is explicitly allowed by the `kv.Store` contract).

`Mutate` is atomic across every process sharing the Redis instance, not just
within one Go process: it uses go-redis's WATCH/MULTI optimistic-transaction
pattern, retrying automatically (bounded) when a concurrent writer touches the
same key.

## Usage

```go
import (
    "context"

    "github.com/redis/go-redis/v9"
    kvredis "github.com/oarkflow/fh-contrib/pkg/storage/kv/redis"
)

// Option A: wrap a client you already own (e.g. one you also use for
// sse/redis or pub/sub). Provider.Close will NOT close it.
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
provider := kvredis.New(client, kvredis.WithPrefix("myapp:"))
defer provider.Close()

// Option B: let the provider build and own its own client.
// Provider.Close WILL close it.
provider, err := kvredis.Open(&redis.Options{Addr: "localhost:6379"})
if err != nil {
    log.Fatal(err)
}
defer provider.Close()

store, err := provider.Store(context.Background(), "ratelimit/public-api")
if err != nil {
    log.Fatal(err)
}
_ = store.Set("some-key", []byte("value"), time.Minute)
```

Wire it into fh the same way any other provider is wired (see
`docs/shared-state.md` in the fh module):

```go
app := fh.New(fh.WithSharedState(provider))
```

## Ownership and Close semantics

- `New(client, opts...)` — you own `client`. `Provider.Close()` marks the
  provider (and every namespace `Store` it issued) closed but never calls
  `client.Close()`, since you may be using that client elsewhere.
- `Open(redisOptions, opts...)` — the provider builds and owns its own
  client (pinging once to fail fast). `Provider.Close()` also closes that
  client.
- An individual `Store.Close()` only marks that one handle closed
  (`kv.ErrClosed` from then on); it never affects sibling namespace stores or
  the shared client. Per `docs/shared-state.md`, close the `Provider`, not
  individual stores — `Store.Close()` exists only because `kv.Store` requires
  the method.

## Production considerations

- **Auth/TLS**: set `Username`/`Password`/`TLSConfig` on the `*redis.Options`
  passed to `Open` (or on the client you build yourself for `New`).
- **Cluster/Sentinel**: this package takes a `*redis.Client` specifically;
  point it at a proxy, or adapt the small amount of `cmdable`-based code here
  to accept `redis.UniversalClient` if you need `redis.ClusterClient` or
  Sentinel failover directly.
- **Key size/connection pool**: configure `PoolSize`, `MaxRetries`, timeouts,
  etc. on `*redis.Options` per your workload; this package does not impose
  its own limits beyond what Redis and go-redis already enforce.
- **Retry budget**: `Mutate`'s optimistic-transaction retry loop is bounded
  (not infinite) so a pathologically hot key fails loudly instead of spinning
  forever under contention.
