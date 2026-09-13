# PostgreSQL kv.Provider for fh

`kvpostgres` implements `github.com/oarkflow/fh/pkg/storage/kv`'s `Provider` and
`Store` interfaces on top of PostgreSQL, using
[`github.com/oarkflow/squealx`](https://github.com/oarkflow/squealx) (a
`database/sql` wrapper) and its pgx-backed postgres driver. It is a drop-in
shared-state backend for any fh middleware that consumes `kv.Store` —
sessions, rate limiters, replay guards, response caches, reputation scores,
and so on — when those need to be durable and shared across replicas rather
than process-local.

## What it does

- One `Provider` owns one backing table (named via `Options.TablePrefix`,
  default `fh_kv_entries`) shared by every namespace it serves. Rows are
  keyed by `(namespace, key)`, so distinct namespaces never see each other's
  entries, and `Len()` is scoped per namespace.
- `Get` treats a lazily-discovered expired row as a miss and opportunistically
  deletes it.
- `Set` upserts with `ON CONFLICT (namespace, key) DO UPDATE`; a `ttl <= 0`
  means the entry never expires on its own.
- `Mutate` is atomic **across every process sharing the database**, not just
  within one Go process: see "How Mutate is made atomic" below.
- The table and its supporting index are created idempotently
  (`CREATE TABLE/INDEX IF NOT EXISTS`) the first time a `Provider` is
  constructed.

## Minimal usage

```go
provider, err := kvpostgres.NewProviderFromDSN(ctx,
    "host=localhost port=5432 user=app password=secret dbname=app sslmode=disable",
    "app-kv", // squealx connection id, for diagnostics
    kvpostgres.Options{},
)
if err != nil {
    return err
}
defer provider.Close()

app := fh.New(fh.WithSharedState(provider))

sessions := session.NewSessionManager(
    app.MustStateStore("sessions/default"),
    session.SessionSecret(sessionSecret),
)
```

## Ownership and Close semantics — two constructors

```go
// 1. Caller-owned connection: Provider.Close does NOT close db. Use this
//    when the application already manages a *squealx.DB (its own pool,
//    metrics, health checks) and wants the kv table to share it.
provider, err := kvpostgres.NewProvider(ctx, db, kvpostgres.Options{})

// 2. Provider-owned connection: Provider.Close DOES close it. Use this when
//    the kv adapter should manage its own connection lifecycle end to end.
provider, err := kvpostgres.NewProviderFromDSN(ctx, dsn, "app-kv", kvpostgres.Options{})
```

Per-namespace `Store.Close()` only marks that one handle closed —
subsequent calls on it return `kv.ErrClosed` — without touching the shared
connection or any sibling `Store` for a different namespace. Only
`Provider.Close()` closes the shared `*squealx.DB`, and only when the
Provider itself opened it. Both `Provider.Close` and `Store.Close` are
idempotent.

## Schema / migration note

```sql
CREATE TABLE IF NOT EXISTS fh_kv_entries (
    namespace  TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      BYTEA,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (namespace, key)
);
CREATE INDEX IF NOT EXISTS fh_kv_entries_ns_exp_idx ON fh_kv_entries (namespace, expires_at);
```

`Options.TablePrefix` (default `fh_kv`) lets one database host more than one
`Provider` instance — e.g. one per environment or bounded-context — without
collision. The prefix is validated as a plain identifier
(`^[A-Za-z_][A-Za-z0-9_]*$`, max 40 chars) because, unlike every other value
in this package, it is interpolated directly into SQL text: table and index
names cannot be bind parameters in PostgreSQL. Never derive it from
user-controlled input.

`key` and `namespace` are `TEXT`. PostgreSQL's text type and wire protocol
cannot represent an embedded NUL byte (`0x00`) in a parameter — this is a
PostgreSQL limitation, not specific to this adapter — so keys/namespaces
containing one will fail with a clear `invalid byte sequence for encoding
"UTF8"` error rather than silently truncating or corrupting data. In
practice this is never an issue for the kinds of values fh middleware uses as
keys (IPs, tokens, cache keys, URLs, session IDs).

If you manage migrations separately from application startup, you can run
the DDL above yourself ahead of time; the constructors' own
`CREATE ... IF NOT EXISTS` calls are then no-ops.

## How Mutate is made atomic

`Mutate` must serialize concurrent read-modify-write callers — across every
process sharing the database, not just goroutines in one process — including
the case where the key does not exist yet. A plain
`SELECT ... FOR UPDATE` cannot do that: there is nothing to lock until a row
exists, so two concurrent callers both creating the same brand-new key can
both observe "absent" and race on the same `INSERT ... ON CONFLICT`.

The fix is a PostgreSQL **advisory transaction lock**, taken unconditionally
at the start of the transaction, before any `SELECT`:

```sql
SELECT pg_advisory_xact_lock(hashtextextended($1, 0) # hashtextextended($2, 1))
-- $1 = namespace, $2 = key
```

This serializes every `Mutate` call for that exact `(namespace, key)` pair,
across every connection and every process, whether or not a row exists yet.
The lock is scoped to the transaction and releases automatically on commit or
rollback — there is no separate unlock call and no way to leak it past the
transaction's lifetime. Inside the lock, `Mutate` does a plain `SELECT`
(no `FOR UPDATE` needed — the advisory lock already serializes), applies the
same expiry check `Get` uses, calls the caller's `fn`, and either upserts
(`ok=true`) or leaves the row untouched (`ok=false`); an error from `fn`
rolls back the transaction and propagates from `Mutate` with no writes made.

`namespace` and `key` are hashed **separately** (with distinct seeds, then
combined with bitwise XOR) rather than concatenated into one string first —
concatenating them in Go would require an unambiguous separator, and since
namespace/key content is arbitrary, no separator byte is truly safe to
choose (and NUL, tempting as a "can't appear in a real key" choice, is
rejected outright by PostgreSQL's text parameters, as noted above). Hashing
each argument independently sidesteps that: neither hash call ever sees a
value that isn't exactly the caller's own namespace or key.

**Caveat:** `hashtextextended` produces a 64-bit hash, so two distinct
`(namespace, key)` pairs can in principle collide onto the same advisory lock
id. A collision only ever costs unrelated keys extra serialization (they
briefly wait on each other's lock) — it can never cause a lost update or
corrupt data, because the actual read/decide/write still operates on the
correct, distinctly-keyed row. At realistic key-space sizes this is a
non-issue.

## Production considerations

- **Connection pool sizing.** `NewProviderFromDSN` applies
  `Options.MaxOpenConns` / `MaxIdleConns` / `ConnMaxLifetime` /
  `ConnMaxIdleTime` (sane defaults: 10 / 5 / 30m / 5m) to the connection it
  opens. `NewProvider` never touches a caller-supplied `*squealx.DB`'s pool
  settings — size that pool yourself with the application's other load in
  mind. `Mutate` holds its connection for the duration of one transaction
  (one advisory lock + one select + at most one write), so a busy `Mutate`
  hot path benefits from headroom beyond the steady-state connection count.
- **Index maintenance.** The `(namespace, expires_at)` index keeps `Len()`'s
  expiry sweep and any future range/administrative queries cheap. Autovacuum
  should be left enabled (the default) — `Set`, `Delete`, `Len`'s sweep, and
  every `Mutate` write produce normal MVCC dead tuples like any other table,
  and this table can be high-churn under a hot rate limiter or cache
  namespace.
- **`QueryTimeout`.** `kv.Store`'s methods don't accept a `context.Context`
  per call (only `Provider.Store` does), so each operation otherwise runs
  under `context.Background()`. Set `Options.QueryTimeout` for defense in
  depth against a wedged connection or a long lock wait blocking a request
  goroutine indefinitely.
- **Least privilege.** The connection only ever needs `SELECT`, `INSERT`,
  `UPDATE`, `DELETE` on its one table (plus `CREATE TABLE`/`CREATE INDEX` at
  startup, or none at all if you provision the schema out-of-band — see
  above). It does not need broader database or superuser privileges.
- **TLS.** Set `sslmode` (and `sslrootcert`/`sslcert`/`sslkey` as needed) in
  the DSN passed to `NewProviderFromDSN`; `NewProvider` inherits whatever TLS
  configuration the caller's `*squealx.DB` was opened with.

## Verification

```bash
go build ./...
go vet ./...
go test -race ./pkg/storage/kv/postgres/...
```

The test suite runs real integration tests against a live PostgreSQL
instance (no mocks) — set `testDSN` in `postgres_test.go` to point at your
own instance if you're not using the default
`host=localhost port=15432 user=postgres password=fhtest dbname=fhkvtest sslmode=disable`.
It covers namespace isolation, TTL expiry (polled, not a fixed sleep),
`Mutate`'s `ok=false` and error paths, both constructors' `Close` semantics,
and — the most important cases — concurrent `Mutate` calls incrementing a
shared counter with no lost updates, both for an existing key and for the
"many first-writers race to create the same brand-new key" case that a naive
row-lock design gets wrong.
