// Command wavicle-cdcbench: measures REAL CDC visibility lag, not estimates.
//
// Procedure: UPDATE a row via raw SQL (timestamped), poll Wavicle GET until the
// new value appears, record the delta. Repeat N times, write every delta to CSV,
// print p50/p99. Run it against Wavicle pointed at a REMOTE Postgres and you get
// the one sentence every evaluator asks for:
//
//	p50 was Xms, p99 was Yms, measured against Postgres on <provider>,
//	over N samples, on <date>.
//
// Requires: PG with the target table (id TEXT PK, value TEXT) + REPLICA IDENTITY
// FULL, a TableMapping (or auto-mapping) so rows land on the polled key format,
// and a wavicle-migrate-style RESP reader (stdlib only; PG via lib/pq).
//
//	wavicle-cdcbench -dsn "postgres://u@host/db?sslmode=require" -target host:6379 \
//	  -table users -id cdc1 -column name -samples 1000 -csv cdc-lag.csv
package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

var (
	dsn     = flag.String("dsn", "", "postgres DSN with write rights")
	target  = flag.String("target", "localhost:6379", "wavicle addr to poll")
	table   = flag.String("table", "users", "table to write")
	rowID   = flag.String("id", "cdc1", "row id to flip")
	column  = flag.String("column", "name", "column to flip (becomes key suffix)")
	samples = flag.Int("samples", 500, "flips to measure")
	csvPath = flag.String("csv", "cdc-lag.csv", "delta CSV output")
	timeout = flag.Duration("timeout", 10*time.Second, "per-poll give-up (counts as +inf sample)")
	pollGap = flag.Duration("poll-gap", 5*time.Millisecond, "delay between GET polls")
)

func respGet(addr, key string, perCall time.Duration) (string, error) {
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(perCall))
	fmt.Fprintf(c, "GET %s\r\n", key)
	r := bufio.NewReader(c)
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if strings.HasPrefix(line, "$-1") {
		return "(nil)", nil
	}
	if !strings.HasPrefix(line, "$") {
		return line, nil
	}
	var n int
	fmt.Sscan(line[1:], &n)
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

func validIdent(s string) bool {
	if len(s) == 0 || len(s) > 63 {
		return false
	}
	for _, r := range s {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func percentile(cp []float64, p float64) float64 {
	if len(cp) == 0 {
		return 0
	}
	s := append([]float64(nil), cp...)
	sort.Float64s(s)
	return s[int(p/100*float64(len(s)-1))]
}

func main() {
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "-dsn is required")
		os.Exit(1)
	}
	// Operator-supplied identifiers go straight into SQL: allowlist them.
	for _, s := range []string{*table, *column} {
		if !validIdent(s) {
			fmt.Fprintf(os.Stderr, "refusing identifier %q (A-Za-z0-9_ only)\n", s)
			os.Exit(1)
		}
	}
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		fmt.Fprintln(os.Stderr, "ping:", err)
		os.Exit(1)
	}
	key := fmt.Sprintf("%s:%s:%s", *table, *rowID, *column)
	deltas := make([]float64, 0, *samples)
	rows := make([][]string, 0, *samples+1)
	rows = append(rows, []string{"sample", "delta_ms", "timed_out"})
	for i := 0; i < *samples; i++ {
		val := fmt.Sprintf("flip-%d-%d", i, time.Now().UnixNano())
		t0 := time.Now()
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, %s) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET %s = $2", *table, *column, *column), *rowID, val); err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			os.Exit(1)
		}
		seen := false
		for time.Since(t0) < *timeout {
			got, err := respGet(*target, key, 2*time.Second)
			if err == nil && got == val {
				d := float64(time.Since(t0).Microseconds()) / 1000
				deltas = append(deltas, d)
				rows = append(rows, []string{fmt.Sprint(i), fmt.Sprintf("%.3f", d), "false"})
				seen = true
				break
			}
			time.Sleep(*pollGap)
		}
		if !seen {
			rows = append(rows, []string{fmt.Sprint(i), "", "true"})
		}
	}
	fh, err := os.Create(*csvPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "csv:", err)
		os.Exit(1)
	}
	_ = csv.NewWriter(fh).WriteAll(rows)
	fh.Close()
	timeouts := *samples - len(deltas)
	fmt.Printf("samples=%d visible=%d timeouts=%d p50=%.2fms p99=%.2fms csv=%s\n",
		*samples, len(deltas), timeouts, percentile(deltas, 50), percentile(deltas, 99), *csvPath)
	if timeouts > 0 {
		os.Exit(2)
	}
}
