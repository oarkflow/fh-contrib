package jwt_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	jwtmw "github.com/oarkflow/fh-contrib/mw/jwt"
	"github.com/oarkflow/securetoken"
)

func startTestServer(t *testing.T, cfg jwtmw.Config) string {
	t.Helper()

	app := fh.New()
	app.Use(jwtmw.New(cfg))
	app.Get("/me", func(c fh.Ctx) error {
		p, ok := fh.PrincipalFrom(c)
		if !ok {
			return c.Status(fh.StatusInternalServerError).SendString("no principal")
		}
		return c.SendString(fmt.Sprintf("%s|%v", p.ID, p.Roles))
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go app.Serve(ln)
	t.Cleanup(func() { ln.Close() })

	return ln.Addr().String()
}

func doGet(t *testing.T, addr, authHeader string) (int, string) {
	t.Helper()

	var resp *http.Response
	var err error
	for i := range 20 {
		req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/me", nil)
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		resp, err = http.DefaultClient.Do(req)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestAuthTokenRoundTrip(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	addr := startTestServer(t, jwtmw.Config{Key: key})

	claims := securetoken.NewAccessClaims("user-1", 5*time.Minute)
	claims.Roles = []string{"admin"}
	tok, err := jwtmw.Sign(claims, key)
	if err != nil {
		t.Fatal(err)
	}

	status, body := doGet(t, addr, "Bearer "+tok)
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	if body != "user-1|[admin]" {
		t.Fatalf("unexpected body %q", body)
	}
}

func TestMissingToken(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	addr := startTestServer(t, jwtmw.Config{Key: key})

	status, _ := doGet(t, addr, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
}

func TestWrongKeyRejected(t *testing.T) {
	signingKey := []byte("0123456789abcdef0123456789abcdef")
	verifyKey := []byte("fedcba9876543210fedcba9876543210")
	addr := startTestServer(t, jwtmw.Config{Key: verifyKey})

	claims := securetoken.NewAccessClaims("user-1", 5*time.Minute)
	tok, err := jwtmw.Sign(claims, signingKey)
	if err != nil {
		t.Fatal(err)
	}

	status, _ := doGet(t, addr, "Bearer "+tok)
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
}

func TestExpiredTokenRejected(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	addr := startTestServer(t, jwtmw.Config{Key: key})

	claims := securetoken.NewAccessClaims("user-1", -1*time.Minute)
	tok, err := jwtmw.Sign(claims, key)
	if err != nil {
		t.Fatal(err)
	}

	status, _ := doGet(t, addr, "Bearer "+tok)
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
}
