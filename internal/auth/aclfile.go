package auth

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadACLFile parses a Redis-style ACL file and returns a ready gate.
// Format (one user per line, `#` comments and blanks ignored):
//
//	user <name> <on|off> [>password] [+CMD|-CMD|+@all ...] [~pattern ...] [reset]
//
// Semantics match Redis where it matters:
//   - users default to OFF unless `on` is present (explicit > implicit)
//   - `>secret` sets the password (file must be 0600 — see runbook)
//   - `+CMD` allows, `-CMD` revokes, `+@all` = admin (all commands, all keys)
//   - `~pattern` allows key prefixes (`users:*`) or `*`
//   - `reset` clears commands/keys/password and turns the user off
//   - unknown tokens are a hard error (fail fast — never boot half-secured)
func LoadACLFile(path string) (*ACL, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("acl file: %w", err)
	}
	defer fh.Close()

	a := &ACL{users: map[string]*User{}}
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 64*1024), 64*1024)
	lineNo := 0
	users := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, u, err := parseACLLine(line)
		if err != nil {
			return nil, fmt.Errorf("acl file %s:%d: %w", path, lineNo, err)
		}
		a.mu.Lock()
		a.users[name] = u
		a.enabled = true
		a.mu.Unlock()
		users++
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("acl file: %w", err)
	}
	if users == 0 {
		return nil, fmt.Errorf("acl file %s: no users defined", path)
	}
	return a, nil
}

func parseACLLine(line string) (string, *User, error) {
	toks := strings.Fields(line)
	if len(toks) < 2 || toks[0] != "user" {
		return "", nil, fmt.Errorf("must start with `user <name>`")
	}
	name := toks[1]
	if name == "" {
		return "", nil, fmt.Errorf("empty user name")
	}
	u := &User{Name: name, Commands: map[string]bool{}, On: false}
	on := false
	for _, t := range toks[2:] {
		switch {
		case t == "on":
			on = true
		case t == "off":
			on = false
		case t == "reset":
			u.Commands = map[string]bool{}
			u.KeyPatterns = nil
			u.PassSHA256 = [32]byte{}
			u.Admin = false
			on = false
		case t == "nopass":
			u.PassSHA256 = [32]byte{}
		case strings.HasPrefix(t, ">"):
			pw := strings.TrimPrefix(t, ">")
			if pw == "" {
				return "", nil, fmt.Errorf("empty password token `>`")
			}
			u.PassSHA256 = hashPass(pw)
		case t == "+@all":
			u.Admin = true
		case t == "-@all":
			u.Admin = false
			u.Commands = map[string]bool{}
		case strings.HasPrefix(t, "+"):
			cmd := strings.ToUpper(strings.TrimPrefix(t, "+"))
			if cmd == "" || strings.HasPrefix(cmd, "@") {
				return "", nil, fmt.Errorf("unsupported selector %q (only +CMD and +@all)", t)
			}
			u.Commands[cmd] = true
		case strings.HasPrefix(t, "-"):
			cmd := strings.ToUpper(strings.TrimPrefix(t, "-"))
			if cmd == "" || strings.HasPrefix(cmd, "@") {
				return "", nil, fmt.Errorf("unsupported selector %q (only -CMD and -@all)", t)
			}
			delete(u.Commands, cmd)
		case strings.HasPrefix(t, "~"):
			u.KeyPatterns = append(u.KeyPatterns, strings.TrimPrefix(t, "~"))
		default:
			return "", nil, fmt.Errorf("unknown token %q", t)
		}
	}
	// Redis semantics: users default to OFF unless explicitly turned on.
	u.On = on
	return name, u, nil
}
