# JWT Authentication Middleware (securetoken)

## What it does

Verifies bearer tokens using `github.com/oarkflow/securetoken` (Ed25519 public,
HMAC-SHA256 auth, or AES-256-GCM local tokens), validates registered claims,
stores claims in `Ctx.Locals`, and sets `fh.Principal`. It is a drop-in
replacement for `github.com/oarkflow/fh/mw/jwt`: same header/scheme
extraction, same `Locals` keys (`jwt_claims`, `jwt_subject`, `jwt_roles`), and
the same `fh.Principal` integration, but verification goes through
securetoken instead of raw JWT algorithm negotiation. Authorization remains in
core helpers or `contrib/mw/authz`.

## How to implement

```go
package main

import (
	"time"

	"github.com/oarkflow/fh"
	jwtmw "github.com/oarkflow/fh/contrib/mw/jwt"
	"github.com/oarkflow/securetoken"
)

func main() {
	key := []byte("dev-secret-please-override-32bytes")

	app := fh.New()
	app.Use(jwtmw.New(jwtmw.Config{Key: key}))

	app.Get("/me", func(c fh.Ctx) error {
		return c.JSON(c.Locals(jwtmw.ClaimsLocalKey))
	})

	// Issue a token the same way an auth service would.
	claims := securetoken.NewAccessClaims("user-42", 15*time.Minute)
	claims.Roles = []string{"admin"}
	token, _ := jwtmw.Sign(claims, key)
	_ = token

	app.Listen(":3000")
}
```

See `examples/jwt` in this repo for a full runnable server plus sample
requests.

## Key/purpose selection

- `Config.Key` — symmetric key for `PurposeAuth` (HMAC-SHA256, the default)
  and `PurposeLocal` (AES-256-GCM) tokens.
- `Config.PublicKey` — Ed25519 public key for `PurposePublic` tokens.
- `Config.KeyFunc` — overrides key resolution entirely, e.g. for kid/footer
  based key rotation via a `securetoken.KeyStore`/`KeyRing`.
- `Config.Purpose` — restricts which protocol prefix (`s1.auth.`,
  `s1.public.`, `s1.local.`) is accepted; defaults to `PurposeAuth`.
- `Config.Policy` — a `securetoken.ValidationPolicy` (issuer, audience,
  leeway, required claims, revocation/replay checks, etc). Use
  `securetoken.StrictAccessPolicy`/`StrictRefreshPolicy` for production
  profiles.

## Impact

Adds cryptographic verification per request. HMAC is fast; Ed25519 signature
verification and revocation/replay store lookups add latency depending on the
backing store.

## Ordering guidance

Run after request ID/tracing/real IP and before authorization, tenant
extraction, and protected handlers.

## Production considerations

Use strong symmetric keys or Ed25519 signing in production, and configure
`Config.Policy` with issuer/audience/expiry validation and a
`RevocationChecker`. securetoken intentionally does not support JWT algorithm
agility (`alg=none`, RS/ES/PS negotiation) or JWKS URLs — tokens must be
issued by trusted first-party code using the same key material configured
here. Do not implement authorization in this middleware; use
`fh.RequireRole`, `fh.RequireScope`, `fh.RequirePermission`, or
`contrib/mw/authz`.
