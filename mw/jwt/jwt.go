// Package jwt adapts github.com/oarkflow/securetoken to github.com/oarkflow/fh
// as a drop-in replacement for github.com/oarkflow/fh/mw/jwt. It keeps the same
// header/scheme extraction, Locals keys, and Principal integration, but verifies
// tokens using securetoken's Ed25519 (public), HMAC-SHA256 (auth), or AES-256-GCM
// (local) protocols instead of raw JWT algorithm negotiation.
//
//	app := fh.New()
//	app.Use(jwt.New(jwt.Config{Key: []byte("dev-secret")}))
//	app.Get("/me", func(c fh.Ctx) error {
//	    return c.JSON(c.Locals(jwt.ClaimsLocalKey))
//	})
package jwt

import (
	"crypto/ed25519"
	"errors"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/securetoken"
)

const ClaimsLocalKey = "jwt_claims"
const SubjectLocalKey = "jwt_subject"
const RolesLocalKey = "jwt_roles"
const SecureClaimsLocalKey = "jwt_secure_claims"

// KeyFunc resolves the verification key for a parsed token. It receives the
// purpose securetoken detected from the token's protocol prefix and must
// return an ed25519.PublicKey (PurposePublic) or a []byte symmetric key
// (PurposeAuth/PurposeLocal). Returning an error rejects the token.
type KeyFunc func(c fh.Ctx, parsed *securetoken.ParsedToken) (any, error)

// ValidateFunc runs after registered-claims validation succeeds, for
// application-specific checks.
type ValidateFunc func(c fh.Ctx, claims *securetoken.Claims) error

type ErrorHandler func(c fh.Ctx, err error) error

type Config struct {
	Header string
	Scheme string

	// Purpose restricts which securetoken protocol is accepted. Defaults to
	// PurposeAuth (HMAC-SHA256), matching fh/mw/jwt's HS256 default. Set to 0
	// via KeyFunc to accept multiple purposes explicitly.
	Purpose securetoken.Purpose

	// Key is the symmetric key used to verify PurposeAuth/PurposeLocal tokens.
	Key []byte
	// PublicKey is the Ed25519 public key used to verify PurposePublic tokens.
	PublicKey ed25519.PublicKey
	// KeyFunc overrides key resolution, e.g. for kid/footer-based key rotation
	// via a securetoken.KeyStore. When set it takes priority over Key/PublicKey.
	KeyFunc KeyFunc

	Policy securetoken.ValidationPolicy

	ClaimsLocal string
	Error       ErrorHandler
	Validate    ValidateFunc
	Next        func(fh.Ctx) bool

	// Principal integration. JWT is authentication only; authorization remains
	// in fh.RequireRole/RequirePermission/RequireScope or contrib/mw/authz.
	DisablePrincipal bool
	PrincipalType    string
}

func New(cfg Config) fh.HandlerFunc {
	applyDefaults(&cfg)
	if cfg.Error == nil {
		cfg.Error = defaultErrorHandler
	}
	keyFunc := cfg.KeyFunc
	if keyFunc == nil {
		keyFunc = defaultKeyFunc(cfg)
	}

	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		token := strings.TrimSpace(c.Get(cfg.Header))
		if token == "" {
			return cfg.Error(c, errors.New("missing token"))
		}
		if cfg.Scheme != "" {
			prefix := cfg.Scheme + " "
			if !strings.HasPrefix(strings.ToLower(token), strings.ToLower(prefix)) {
				return cfg.Error(c, errors.New("invalid token scheme"))
			}
			token = strings.TrimSpace(token[len(prefix):])
		}

		var claims securetoken.Claims
		parsed, err := securetoken.ParseWithClaims(c.Context(), token, &claims, func(pt *securetoken.ParsedToken) (any, error) {
			return keyFunc(c, pt)
		}, cfg.Policy)
		if err != nil {
			return cfg.Error(c, err)
		}
		if !parsed.Valid {
			return cfg.Error(c, errors.New("invalid token"))
		}
		if cfg.Validate != nil {
			if err := cfg.Validate(c, &claims); err != nil {
				return cfg.Error(c, err)
			}
		}

		c.Locals(cfg.ClaimsLocal, claimsToMap(claims))
		c.Locals(SecureClaimsLocalKey, &claims)
		if claims.Subject != "" {
			c.Locals(SubjectLocalKey, claims.Subject)
		}
		if len(claims.Roles) > 0 {
			c.Locals(RolesLocalKey, claims.Roles)
		}
		if !cfg.DisablePrincipal {
			fh.SetPrincipal(c, principalFromClaims(claims, cfg))
		}

		return c.Next()
	}
}

func defaultKeyFunc(cfg Config) KeyFunc {
	return func(c fh.Ctx, parsed *securetoken.ParsedToken) (any, error) {
		if cfg.Purpose != 0 && parsed.Purpose != cfg.Purpose {
			return nil, securetoken.ErrInvalidPurpose
		}
		switch parsed.Purpose {
		case securetoken.PurposePublic:
			if len(cfg.PublicKey) == 0 {
				return nil, securetoken.ErrInvalidKey
			}
			return cfg.PublicKey, nil
		case securetoken.PurposeAuth, securetoken.PurposeLocal:
			if len(cfg.Key) == 0 {
				return nil, securetoken.ErrInvalidKey
			}
			return cfg.Key, nil
		default:
			return nil, securetoken.ErrInvalidPurpose
		}
	}
}

func defaultErrorHandler(c fh.Ctx, err error) error {
	return c.Status(fh.StatusUnauthorized).JSON(fh.Map{"error": "jwt_invalid", "message": err.Error()})
}

func applyDefaults(cfg *Config) {
	if cfg.Header == "" {
		cfg.Header = "Authorization"
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "Bearer"
	}
	if cfg.ClaimsLocal == "" {
		cfg.ClaimsLocal = ClaimsLocalKey
	}
	if cfg.Purpose == 0 {
		cfg.Purpose = securetoken.PurposeAuth
	}
	if cfg.PrincipalType == "" {
		cfg.PrincipalType = "user"
	}
}

func principalFromClaims(claims securetoken.Claims, cfg Config) fh.Principal {
	return fh.Principal{
		ID:         claims.Subject,
		Type:       cfg.PrincipalType,
		TenantID:   claims.TenantID,
		Subject:    claims.Subject,
		Roles:      claims.Roles,
		Scopes:     claims.Scope,
		Claims:     claimsToMap(claims),
		AuthMethod: "jwt",
	}
}

func claimsToMap(c securetoken.Claims) map[string]any {
	m := make(map[string]any, 13)
	if c.Issuer != "" {
		m["iss"] = c.Issuer
	}
	if c.Subject != "" {
		m["sub"] = c.Subject
	}
	if len(c.Audience) > 0 {
		m["aud"] = c.Audience
	}
	if c.ExpiresAt != 0 {
		m["exp"] = c.ExpiresAt
	}
	if c.NotBefore != 0 {
		m["nbf"] = c.NotBefore
	}
	if c.IssuedAt != 0 {
		m["iat"] = c.IssuedAt
	}
	if c.ID != "" {
		m["jti"] = c.ID
	}
	if c.KeyID != "" {
		m["kid"] = c.KeyID
	}
	if c.Type != "" {
		m["typ"] = c.Type
	}
	if len(c.Scope) > 0 {
		m["scope"] = c.Scope
	}
	if len(c.Roles) > 0 {
		m["roles"] = c.Roles
	}
	if c.TenantID != "" {
		m["tenant_id"] = c.TenantID
	}
	if c.SessionID != "" {
		m["sid"] = c.SessionID
	}
	if c.FamilyID != "" {
		m["fid"] = c.FamilyID
	}
	return m
}

// Sign creates a securetoken auth (HMAC-SHA256) token for tests, examples, and
// service-to-service calls. Use securetoken.SignJSON directly for Ed25519
// public tokens, or securetoken.EncryptJSON for AES-256-GCM local tokens.
func Sign(claims securetoken.Claims, key []byte) (string, error) {
	if len(key) == 0 {
		return "", errors.New("missing signing key")
	}
	return securetoken.AuthSignClaims(key, &claims, securetoken.TokenOptions{})
}

// SecureClaimsFromCtx returns the typed securetoken.Claims stored by New, for
// callers that don't want the map[string]any view under ClaimsLocal.
func SecureClaimsFromCtx(c fh.Ctx) (securetoken.Claims, bool) {
	claims, ok := c.Locals(SecureClaimsLocalKey).(*securetoken.Claims)
	if !ok || claims == nil {
		return securetoken.Claims{}, false
	}
	return *claims, true
}
