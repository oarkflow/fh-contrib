// Command jwt runs an fh server protected by contrib/mw/jwt (backed by
// github.com/oarkflow/securetoken) and prints a couple of sample requests
// against it so you can see the middleware working end to end.
//
//	go run ./examples/jwt
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/oarkflow/fh"
	jwtmw "github.com/oarkflow/fh/contrib/mw/jwt"
	"github.com/oarkflow/securetoken"
)

// devKey is a stand-in for a secret loaded from your environment/KMS.
var devKey = []byte("dev-secret-please-override-32bytes")

func main() {
	app := fh.New()

	app.Use(jwtmw.New(jwtmw.Config{
		Key: devKey,
	}))

	app.Get("/me", func(c fh.Ctx) error {
		p, ok := fh.PrincipalFrom(c)
		if !ok {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{"error": "no principal"})
		}
		return c.JSON(fh.Map{
			"id":     p.ID,
			"roles":  p.Roles,
			"claims": c.Locals(jwtmw.ClaimsLocalKey),
		})
	})

	addr := "127.0.0.1:3000"
	go func() {
		if err := app.Listen(addr); err != nil {
			log.Println("server stopped:", err)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	demo(addr)
}

func demo(addr string) {
	// Issue a token the same way an auth service would: sign a securetoken
	// access token with the shared HMAC key.
	claims := securetoken.NewAccessClaims("user-42", 15*time.Minute)
	claims.Roles = []string{"admin"}
	token, err := jwtmw.Sign(claims, devKey)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("== authenticated request ==")
	get(addr, "Bearer "+token)

	fmt.Println("== missing token ==")
	get(addr, "")
}

func get(addr, authHeader string) {
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/me", nil)
	if err != nil {
		log.Fatal(err)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("status=%d body=%s\n\n", resp.StatusCode, body)
}
