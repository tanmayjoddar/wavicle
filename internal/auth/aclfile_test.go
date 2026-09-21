package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func writeACL(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "users.acl")
	if err := os.WriteFile(p, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestACLFile_Basic(t *testing.T) {
	p := writeACL(t, `
# comment + blank lines ignored

user admin on >admin-secret +@all ~*
user app on >app-secret +GET +SET ~users:* ~profile:*
user dead off >x +GET ~*
`)
	a, err := LoadACLFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Authenticate("admin", "admin-secret") {
		t.Fatal("admin should auth")
	}
	if a.Authenticate("admin", "wrong") {
		t.Fatal("wrong password must fail")
	}
	if !a.Authenticate("app", "app-secret") {
		t.Fatal("app should auth")
	}
	if a.Authenticate("dead", "x") {
		t.Fatal("off user must fail even with right password")
	}
	if a.Authenticate("ghost", "x") {
		t.Fatal("unknown user must fail")
	}
	if err := a.Authorize("app", "GET", "users:1:name"); err != nil {
		t.Fatalf("app GET users:* allowed: %v", err)
	}
	if err := a.Authorize("app", "FLUSHDB", "users:1:name"); err == nil {
		t.Fatal("app FLUSHDB must be denied")
	}
	if err := a.Authorize("app", "GET", "payments:1"); err == nil {
		t.Fatal("app payments:* must be denied")
	}
	if err := a.Authorize("admin", "FLUSHDB", "anything"); err != nil {
		t.Fatalf("admin allowed all: %v", err)
	}
	if err := a.Authorize("dead", "GET", "users:1"); err == nil {
		t.Fatal("off user must be denied at authorize too")
	}
}

func TestACLFile_PlusMinus(t *testing.T) {
	p := writeACL(t, "user w on >p +GET +SET -SET ~*\n")
	a, err := LoadACLFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Authorize("w", "GET", "k"); err != nil {
		t.Fatalf("GET allowed: %v", err)
	}
	if err := a.Authorize("w", "SET", "k"); err == nil {
		t.Fatal("SET revoked by -SET")
	}
}

func TestACLFile_Errors(t *testing.T) {
	for _, body := range []string{
		"admin on >x +@all ~*\n",          // missing `user` keyword
		"user bad on >x wut ~*\n",         // unknown bare token
		"user bad on > ~*\n",              // empty password
		"user bad on >x +@write ~*\n",     // unsupported @ selector
		"# only a comment\n",              // no users at all
	} {
		if _, err := LoadACLFile(writeACL(t, body)); err == nil {
			t.Fatalf("expected error for %q", body)
		}
	}
	if _, err := LoadACLFile(filepath.Join(t.TempDir(), "missing.acl")); err == nil {
		t.Fatal("missing file must error")
	}
}
