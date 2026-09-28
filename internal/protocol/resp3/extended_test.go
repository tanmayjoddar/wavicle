package resp3

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"wavicle/internal/storage"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	crystal, err := storage.NewCausalCrystal(filepath.Join(t.TempDir(), "ext.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { crystal.Close() })
	return NewServer(crystal, "", 100)
}

func mustOK(t *testing.T, s *Server, args ...string) string {
	t.Helper()
	resp, err := s.HandleCommand(args)
	if err != nil {
		t.Fatalf("%v -> err %v", args, err)
	}
	return resp
}

func TestExtended_Lists(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "RPUSH", "mylist", "a", "b", "c")
	if r := mustOK(t, s, "LLEN", "mylist"); !strings.Contains(r, ":3") {
		t.Fatalf("LLEN=%q", r)
	}
	if r := mustOK(t, s, "LRANGE", "mylist", "0", "-1"); !strings.Contains(r, "a") || !strings.Contains(r, "c") {
		t.Fatalf("LRANGE=%q", r)
	}
	if r := mustOK(t, s, "LPOP", "mylist"); !strings.Contains(r, "a") {
		t.Fatalf("LPOP=%q", r)
	}
}

func TestExtended_SetsZSets(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "SADD", "myset", "x", "y")
	if r := mustOK(t, s, "SCARD", "myset"); !strings.Contains(r, ":2") {
		t.Fatalf("SCARD=%q", r)
	}
	if r := mustOK(t, s, "SISMEMBER", "myset", "x"); !strings.Contains(r, ":1") {
		t.Fatalf("SISMEMBER=%q", r)
	}
	mustOK(t, s, "ZADD", "myz", "1", "a", "2", "b")
	if r := mustOK(t, s, "ZSCORE", "myz", "b"); !strings.Contains(r, "2") {
		t.Fatalf("ZSCORE=%q", r)
	}
	if r := mustOK(t, s, "ZRANGE", "myz", "0", "-1"); !strings.Contains(r, "a") {
		t.Fatalf("ZRANGE=%q", r)
	}
}

func TestExtended_CountersHash(t *testing.T) {
	s := newTestServer(t)
	if r := mustOK(t, s, "INCR", "cnt"); !strings.Contains(r, ":1") {
		t.Fatalf("INCR=%q", r)
	}
	if r := mustOK(t, s, "INCRBY", "cnt", "9"); !strings.Contains(r, ":10") {
		t.Fatalf("INCRBY=%q", r)
	}
	mustOK(t, s, "HMSET", "u:1", "name", "Al", "age", "30")
	if r := mustOK(t, s, "HMGET", "u:1", "name", "age"); !strings.Contains(r, "Al") {
		t.Fatalf("HMGET=%q", r)
	}
	if r := mustOK(t, s, "HLEN", "u:1"); !strings.Contains(r, ":2") {
		t.Fatalf("HLEN=%q", r)
	}
}

func TestRegression_KEYS_CollapsesHash(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "HSET", "profile:1", "theme", "dark")
	r := mustOK(t, s, "KEYS", "profile:*")
	if !strings.Contains(r, "profile:1") || strings.Contains(r, "profile:1:theme") {
		t.Fatalf("KEYS leak internals: %q", r)
	}
}

func TestRegression_KEYS_FlatColonKeysIntact(t *testing.T) {
	// Caught live by the shadow runner: 200 shadow:N keys collapsed to one
	// phantom "shadow" by a heuristic that couldn't tell hash fields from
	// flat colon keys. Only explicit-hash parents may collapse.
	s := newTestServer(t)
	for i := 0; i < 50; i++ {
		mustOK(t, s, "SET", fmt.Sprintf("shadow:%d", i), "v")
	}
	mustOK(t, s, "HSET", "profile:1", "theme", "dark")
	r := mustOK(t, s, "KEYS", "shadow:*")
	for i := 0; i < 50; i++ {
		if !strings.Contains(r, fmt.Sprintf("shadow:%d", i)) {
			t.Fatalf("flat key missing from KEYS: %q", r)
		}
	}
	r2 := mustOK(t, s, "KEYS", "*")
	if !strings.Contains(r2, "profile:1") || strings.Contains(r2, "profile:1:theme") {
		t.Fatalf("hash collapse regressed: %q", r2)
	}
}

func TestRegression_SetZSetIsolation(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "SADD", "mix", "x")
	if r := mustOK(t, s, "ZADD", "mix", "1", "a"); !strings.Contains(r, "WRONGTYPE") {
		t.Fatalf("ZADD over SET must WRONGTYPE, got %q", r)
	}
	mustOK(t, s, "ZADD", "mix2", "1", "a")
	if r := mustOK(t, s, "SADD", "mix2", "y"); !strings.Contains(r, "WRONGTYPE") {
		t.Fatalf("SADD over ZSET must WRONGTYPE, got %q", r)
	}
}

func TestRegression_RPOPOrder(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "RPUSH", "lst", "a", "b", "c", "d")
	r := mustOK(t, s, "RPOP", "lst", "2")
	d, c := strings.Index(r, "d"), strings.Index(r, "c")
	if d < 0 || c < 0 || d > c {
		t.Fatalf("RPOP 2 must return d then c, got %q", r)
	}
}

func TestRegression_XRANGEFilter(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "XADD", "stm", "1-1", "f", "v1")
	mustOK(t, s, "XADD", "stm", "2-1", "f", "v2")
	mustOK(t, s, "XADD", "stm", "3-1", "f", "v3")
	r := mustOK(t, s, "XRANGE", "stm", "2-1", "2-1")
	if !strings.Contains(r, "2-1") || strings.Contains(r, "1-1") || strings.Contains(r, "3-1") {
		t.Fatalf("XRANGE must filter to 2-1 only, got %q", r)
	}
}

func TestStaleGuard_DisabledByDefault(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "SET", "k", "v")
	mustOK(t, s, "GET", "k") // no guard configured — fail-open, always serves
}

func TestStaleGuard_PGSlotHealth(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "SET", "k", "v")
	state := &atomic.Int64{}
	s.SetStaleGuardPG(state, 10<<30)

	// Unknown (boot, monitor hasn't reported): grace, serves.
	state.Store(-1)
	mustOK(t, s, "GET", "k")

	// Healthy-but-quiet: lag 0 with NO events ever — idle streams SERVE.
	// (This is the case event-recency oracles get wrong.)
	state.Store(0)
	mustOK(t, s, "GET", "k")

	// Slot-proven behind: fail closed.
	state.Store(10 << 30)
	if _, err := s.HandleCommand([]string{"GET", "k"}); err == nil || !strings.Contains(err.Error(), "STALE") {
		t.Fatalf("page-level lag must fail closed, got err=%v", err)
	}
	// Slot blind (missing/inactive): fail closed.
	state.Store(-2)
	if _, err := s.HandleCommand([]string{"GET", "k"}); err == nil || !strings.Contains(err.Error(), "STALE") {
		t.Fatalf("broken slot must fail closed, got err=%v", err)
	}
	// Recovery: healthy again → serves.
	state.Store(0)
	mustOK(t, s, "GET", "k")

	// Writes are never gated — blocking writes turns staleness into outage.
	state.Store(-2)
	mustOK(t, s, "SET", "k2", "v2")
}

func TestStaleGuard_MySQLPollHealth(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "SET", "k", "v")
	last := &atomic.Int64{}
	s.SetStaleGuardMySQL(last, 50*time.Millisecond)

	// Never polled (0): grace, serves. Fresh poll: serves.
	mustOK(t, s, "GET", "k")
	last.Store(time.Now().UnixNano())
	mustOK(t, s, "GET", "k")

	// Poller silent past bound: fail closed.
	last.Store(time.Now().Add(-time.Second).UnixNano())
	if _, err := s.HandleCommand([]string{"GET", "k"}); err == nil || !strings.Contains(err.Error(), "STALE") {
		t.Fatalf("dead poller must fail closed, got err=%v", err)
	}
	// TYPE is keyless metadata, never gated.
	if _, err := s.HandleCommand([]string{"TYPE", "k"}); err != nil {
		t.Fatalf("TYPE must not gate: %v", err)
	}
}

func TestStaleGuard_BootGrace(t *testing.T) {
	s := newTestServer(t)
	mustOK(t, s, "SET", "k", "v")
	// True "unknown" is -1, set explicitly by main at boot; a fresh zero
	// atomic reads as lag 0 (healthy). Both must serve.
	unknown := &atomic.Int64{}
	unknown.Store(-1)
	s.SetStaleGuardPG(unknown, 10<<30)
	mustOK(t, s, "GET", "k")
	s.SetStaleGuardPG(&atomic.Int64{}, 10<<30)
	mustOK(t, s, "GET", "k")
}
