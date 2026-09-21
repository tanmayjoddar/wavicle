package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"strings"
	"sync"
)

// User is one ACL principal. Passwords stored as SHA256 hex (never plaintext
// in memory beyond startup). Commands are uppercase allowlist; empty means all.
// KeyPatterns are prefix globs ("users:*", "*"); empty means all keys.
type User struct {
	Name        string
	PassSHA256  [32]byte
	Commands    map[string]bool
	KeyPatterns []string
	Admin       bool
	On          bool // explicit on/off switch (Redis semantics: default off)
}

// ACL is the production auth gate. Goroutine-safe, hot-path lock-free (RLock).
type ACL struct {
	mu      sync.RWMutex
	users   map[string]*User
	enabled bool
}

func NewDisabled() *ACL { return &ACL{users: map[string]*User{}} }

// NewSinglePassword preserves backward compat: one shared password, full access.
func NewSinglePassword(password string) *ACL {
	if password == "" {
		return NewDisabled()
	}
	a := &ACL{users: map[string]*User{}, enabled: true}
	a.AddUser("default", password, nil, nil, true)
	return a
}

func hashPass(pw string) [32]byte { return sha256.Sum256([]byte(pw)) }

func (a *ACL) AddUser(name, password string, commands []string, keyPatterns []string, admin bool) {
	cmds := map[string]bool{}
	for _, c := range commands {
		cmds[strings.ToUpper(c)] = true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.users[name] = &User{Name: name, PassSHA256: hashPass(password), Commands: cmds, KeyPatterns: keyPatterns, Admin: admin, On: true}
	a.enabled = true
}

func (a *ACL) Enabled() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.enabled
}

// Authenticate constant-time compares password hash.
// Users switched OFF (or unknown) always fail, even with the right password.
func (a *ACL) Authenticate(name, password string) bool {
	a.mu.RLock()
	u, ok := a.users[name]
	a.mu.RUnlock()
	if !ok || !u.On {
		return false
	}
	h := hashPass(password)
	return subtle.ConstantTimeCompare(u.PassSHA256[:], h[:]) == 1
}

// Authorize checks command + first-key against user policy.
// key may be "" for keyless commands (PING/INFO/DBSIZE).
func (a *ACL) Authorize(name, cmd, key string) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.enabled {
		return nil
	}
	u, ok := a.users[name]
	if !ok || !u.On {
		return fmt.Errorf("NOAUTH unknown user")
	}
	if u.Admin {
		return nil
	}
	uc := strings.ToUpper(cmd)
	if len(u.Commands) > 0 && !u.Commands[uc] && !u.Commands["ALL"] {
		return fmt.Errorf("NOPERM command %s denied for user %s", uc, name)
	}
	if key != "" && len(u.KeyPatterns) > 0 {
		for _, p := range u.KeyPatterns {
			if matchPattern(p, key) {
				return nil
			}
		}
		return fmt.Errorf("NOPERM key %q denied for user %s", key, name)
	}
	return nil
}

func matchPattern(pat, key string) bool {
	if pat == "*" || pat == "" {
		return true
	}
	if strings.HasSuffix(pat, "*") {
		return strings.HasPrefix(key, strings.TrimSuffix(pat, "*"))
	}
	return pat == key
}
