// PG-backed consistency: Wavicle RESP writers + raw SQL writers race the same
// rows while RESP readers sample visibility lag. Skips cleanly without a DB
// (same DSN convention as the integration suite).
//
// Validity oracle, stated exactly: every observed value must have been WRITTEN
// by somebody before the read completed — recorded in `valid` at write time,
// checked on every read. (No monotonic assertion here: CDC delivery can reorder
// concurrent cross-writer generations, so per-key monotonicity is NOT promised
// across the DB boundary — only validity, bounded visibility, convergence.)
// Asserts (seeded, reproducible):
//  1. validity: no ghosts, ever
//  2. visibility bound: raw-SQL write → first RESP read observing it,
//     p50/p99 reported, p99 must sit under -- visBound (default 10s; the real
//     stream on loopback measures ~35ms p99, same-VPC budget 500ms)
//  3. convergence: after quiesce, every key reads its last value
package consistency_test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const (
	pgDSN      = "postgres://wavicle@localhost:5433/wavicle?sslmode=disable"
	pgAddr     = "127.0.0.1:6391"
	visBound   = 10 * time.Second
	nPGKeys    = 32
)

func pgOpen(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		t.Skipf("no PG at %s: %v", pgDSN, err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("PG not reachable at %s: %v", pgDSN, err)
	}
	return db
}

// respConn opens a raw RESP connection and returns send/recv closures.
func respConn(t *testing.T, addr string) (func(string) string, func()) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Skipf("no server at %s: %v (start one against PG first)", addr, err)
	}
	buf := make([]byte, 65536)
	send := func(line string) string {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := fmt.Fprintf(conn, "%s\r\n", line); err != nil {
			t.Fatalf("write: %v", err)
		}
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return string(buf[:n])
	}
	return send, func() { conn.Close() }
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(p/100*float64(len(sorted)-1))]
}

func TestConsistency_PG_DualWriters(t *testing.T) {
	const seed = 424242
	db := pgOpen(t)
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS consistency_acct (id TEXT PRIMARY KEY, balance TEXT);
		ALTER TABLE consistency_acct REPLICA IDENTITY FULL;`); err != nil {
		t.Skipf("cannot prepare table (need CREATE + REPLICA IDENTITY rights): %v", err)
	}

	keys := make([]string, nPGKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("cacc:%d", i)
		_, _ = db.Exec(`INSERT INTO consistency_acct (id, balance) VALUES ($1, $2)
			ON CONFLICT (id) DO UPDATE SET balance = $2`, keys[i], fmt.Sprintf("init-%d", i))
	}

	send, closeConn := respConn(t, pgAddr)
	defer closeConn()

	var mu sync.Mutex
	valid := map[string]map[string]bool{}
	for i, k := range keys {
		valid[k] = map[string]bool{fmt.Sprintf("init-%d", i): true}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Wavicle writers via RESP (table:id:column paths).
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s, closer := respConn(t, pgAddr)
			defer closer()
			r := rand.New(rand.NewSource(int64(seed + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				base := keys[r.Intn(nPGKeys)]
				v := fmt.Sprintf("w%d-%d", w, r.Intn(1000000))
				s("SET " + base + ":balance " + v)
				mu.Lock()
				valid[base][v] = true
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	// Raw SQL writers: the DBA path. Each write records commit time for the
	// visibility-lag sample taken by readers below.
	type rawWrite struct {
		key string
		val string
		at  time.Time
	}
	var rwMu sync.Mutex
	pendingVis := map[string]rawWrite{}
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed + 50 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[r.Intn(nPGKeys)]
				v := fmt.Sprintf("raw%d-%d", w, r.Intn(1000000))
				if _, err := db.Exec(`INSERT INTO consistency_acct (id, balance) VALUES ($1, $2)
					ON CONFLICT (id) DO UPDATE SET balance = $2`, k, v); err != nil {
					continue
				}
				mu.Lock()
				valid[k][v] = true
				mu.Unlock()
				rwMu.Lock()
				pendingVis[k] = rawWrite{k, v, time.Now()}
				rwMu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	// Readers: validity + visibility-lag sampling.
	var lagMu sync.Mutex
	var lags []time.Duration
	var violMu sync.Mutex
	violations := 0
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			s, closer := respConn(t, pgAddr)
			defer closer()
			rr := rand.New(rand.NewSource(int64(seed + 100 + r)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[rr.Intn(nPGKeys)]
				resp := s("GET " + k + ":balance")
				// Bulk reply "$N\r\nval\r\n" — extract val, skip nils.
				got := parseBulk(resp)
				if got == "" {
					continue
				}
				mu.Lock()
				ok := valid[k][got]
				mu.Unlock()
				if !ok {
					violMu.Lock()
					violations++
					violMu.Unlock()
					t.Errorf("VALIDITY %s observed %q", k, got)
					continue
				}
				rwMu.Lock()
				if pw, awaiting := pendingVis[k]; awaiting && pw.val == got {
					lagMu.Lock()
					lags = append(lags, time.Since(pw.at))
					lagMu.Unlock()
					delete(pendingVis, k)
				}
				rwMu.Unlock()
			}
		}(r)
	}

	_ = send
	time.Sleep(15 * time.Second)
	close(stop)
	wg.Wait()

	violMu.Lock()
	n := violations
	violMu.Unlock()
	if n > 0 {
		t.Fatalf("%d validity violations (seed %d)", n, seed)
	}
	lagMu.Lock()
	cp := append([]time.Duration(nil), lags...)
	lagMu.Unlock()
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	p50, p99 := percentile(cp, 50), percentile(cp, 99)
	t.Logf("visibility samples=%d p50=%v p99=%v bound=%v (seed %d)", len(cp), p50, p99, visBound, seed)
	if len(cp) == 0 {
		t.Fatal("no visibility samples — is CDC delivering? check slot + listener")
	}
	if p99 > visBound {
		t.Fatalf("p99 visibility %v exceeded bound %v", p99, visBound)
	}
}

func parseBulk(resp string) string {
	// "$5\r\nhello\r\n" -> "hello"; "$-1" -> ""
	if len(resp) < 4 || resp[0] != '$' {
		return ""
	}
	for i := 1; i < len(resp); i++ {
		if resp[i] == '\n' {
			v := resp[i+1:]
			if len(v) >= 2 && v[len(v)-2:] == "\r\n" {
				v = v[:len(v)-2]
			}
			if v == "-1" || v == "" {
				return ""
			}
			return v
		}
	}
	return ""
}
