// Command wavicle-migrate is the shadow-migration prover.
// It dual-reads source (Redis-compatible) vs target (Wavicle) and diffs:
//
//	wavicle-migrate -source localhost:6379 -target localhost:6380 -pattern "users:*" -count 1000
//
// Exit 0 = all keys match. Exit 2 = mismatches (proves stale or drift).
// Uses only stdlib raw RESP so it works against real Redis, Dragonfly, or Wavicle.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	src := flag.String("source", "localhost:6379", "source Redis-compatible addr")
	dst := flag.String("target", "localhost:6380", "target Wavicle addr")
	pattern := flag.String("pattern", "*", "KEYS pattern")
	count := flag.Int("count", 10000, "max keys to compare")
	timeout := flag.Duration("timeout", 5*time.Second, "dial timeout")
	authPass := flag.String("auth", "", "password sent as AUTH on both ends (pilot stacks require it)")
	continuous := flag.Bool("continuous", false, "loop sweeps until -duration or SIGINT (shadow-read mode)")
	interval := flag.Duration("interval", 30*time.Second, "delay between sweeps in continuous mode")
	duration := flag.Duration("duration", 0*time.Second, "total run time in continuous mode (0 = until SIGINT)")
	getTimeout := flag.Duration("get-timeout", 50*time.Millisecond, "hard per-GET deadline; overruns count as Wavicle-side failures, never block the loop")
	logPath := flag.String("log", "", "append JSON mismatch lines here (continuous mode)")
	reportPath := flag.String("report", "shadow-report.json", "write summary report JSON here on exit (continuous mode)")
	flag.Parse()

	// Pilot stacks require AUTH; handshake once per connection when set.
	authPassword = *authPass

	if !*continuous {
		runOnce(*src, *dst, *pattern, *count, *timeout)
		return
	}
	runContinuous(*src, *dst, *pattern, *count, *timeout, *interval, *duration, *getTimeout, *logPath, *reportPath)
}

// authPassword, when non-empty, is sent as AUTH immediately after connect.
var authPassword string

func runOnce(src, dst, pattern string, count int, timeout time.Duration) {
	keys, err := respKeys(src, pattern, timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "source KEYS failed: %v\n", err)
		os.Exit(1)
	}
	sort.Strings(keys)
	if len(keys) > count {
		keys = keys[:count]
	}
	fmt.Printf("comparing %d keys pattern=%q\n", len(keys), pattern)

	mismatch := 0
	checked := 0
	for _, k := range keys {
		a, _, errA := respGet(src, k, timeout, timeout*3)
		b, _, errB := respGet(dst, k, timeout, timeout*3)
		checked++
		if errA != nil || errB != nil || a != b {
			mismatch++
			fmt.Printf("DIFF key=%q src=%q(srcErr=%v) dst=%q(dstErr=%v)\n", k, a, errA, b, errB)
		}
	}
	fmt.Printf("checked=%d mismatch=%d match_rate=%.2f%%\n", checked, mismatch, pct(checked, mismatch))
	if mismatch > 0 {
		os.Exit(2)
	}
}

func pct(checked, mismatch int) float64 {
	if checked == 0 {
		return 100
	}
	return float64(checked-mismatch) * 100 / float64(checked)
}

func dial(addr string, timeout time.Duration) (net.Conn, *bufio.Reader, *bufio.Writer, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, nil, nil, err
	}
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	if authPassword != "" {
		_ = c.SetDeadline(time.Now().Add(timeout))
		hdr, _, err := sendInline(w, r, "AUTH "+authPassword)
		if err != nil || !strings.HasPrefix(hdr, "+OK") {
			c.Close()
			if err == nil {
				err = fmt.Errorf("AUTH rejected: %q", hdr)
			}
			return nil, nil, nil, err
		}
	}
	return c, r, w, nil
}

func sendInline(w *bufio.Writer, r *bufio.Reader, line string) (string, []string, error) {
	if _, err := fmt.Fprintf(w, "%s\r\n", line); err != nil {
		return "", nil, err
	}
	if err := w.Flush(); err != nil {
		return "", nil, err
	}
	return readReply(r)
}

func readReply(r *bufio.Reader) (string, []string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", nil, io.ErrUnexpectedEOF
	}
	typ := line[0]
	rest := line[1:]
	switch typ {
	case '+', '-', ':':
		return line, nil, nil
	case '$':
		var n int
		_, _ = fmt.Sscan(rest, &n)
		if n == -1 {
			return line, nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", nil, err
		}
		return line, []string{string(buf[:n])}, nil
	case '*':
		var n int
		_, _ = fmt.Sscan(rest, &n)
		if n <= 0 {
			return line, nil, nil
		}
		var out []string
		for i := 0; i < n; i++ {
			hdr, err := r.ReadString('\n')
			if err != nil {
				return "", nil, err
			}
			hdr = strings.TrimRight(hdr, "\r\n")
			if !strings.HasPrefix(hdr, "$") {
				out = append(out, hdr)
				continue
			}
			var m int
			_, _ = fmt.Sscan(hdr[1:], &m)
			if m == -1 {
				out = append(out, "")
				continue
			}
			buf := make([]byte, m+2)
			if _, err := io.ReadFull(r, buf); err != nil {
				return "", nil, err
			}
			out = append(out, string(buf[:m]))
		}
		return line, out, nil
	case '%', '#', '_', ',', '(':
		return line, nil, nil
	default:
		return line, nil, nil
	}
}

func respKeys(addr, pattern string, timeout time.Duration) ([]string, error) {
	c, r, w, err := dial(addr, timeout)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(timeout * 3))
	hdr, bulk, err := sendInline(w, r, "KEYS "+pattern)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(hdr, "*") {
		return bulk, nil
	}
	return nil, fmt.Errorf("unexpected KEYS reply %q", hdr)
}

// respGet issues one GET with a HARD per-call deadline. Overruns return an
// error — callers count it as a target-side failure, never block on it.
func respGet(addr, key string, dialTimeout, callTimeout time.Duration) (val string, lat time.Duration, err error) {
	c, r, w, err := dial(addr, dialTimeout)
	if err != nil {
		return "", 0, err
	}
	defer c.Close()
	t0 := time.Now()
	_ = c.SetDeadline(t0.Add(callTimeout))
	hdr, bulk, err := sendInline(w, r, "GET "+escapeArg(key))
	lat = time.Since(t0)
	if err != nil {
		return "", lat, err
	}
	if strings.HasPrefix(hdr, "$-1") {
		return "(nil)", lat, nil
	}
	if len(bulk) > 0 {
		return bulk[0], lat, nil
	}
	return hdr, lat, nil
}

func escapeArg(s string) string {
	if strings.ContainsAny(s, " \r\n") {
		return `"` + s + `"`
	}
	return s
}

// mismatchLine is one JSONL record in the mismatch log.
type mismatchLine struct {
	Timestamp   string  `json:"ts"`
	Key         string  `json:"key"`
	Source      string  `json:"source_value"`
	Target      string  `json:"target_value"`
	TargetError string  `json:"target_error,omitempty"`
	SourceMs    float64 `json:"source_ms"`
	TargetMs    float64 `json:"target_ms"`
}

// shadowReport is the summary written on SIGINT / duration end.
type shadowReport struct {
	StartedAt       string  `json:"started_at"`
	DurationS       float64 `json:"duration_s"`
	Sweeps          int64   `json:"sweeps"`
	ComparedTotal   int64   `json:"compared_total"`
	MismatchTotal   int64   `json:"mismatch_total"`
	MatchRatePct    float64 `json:"match_rate_pct"`
	WavicleErrTotal int64   `json:"wavicle_error_total"`
	SourceP50Ms     float64 `json:"source_p50_ms"`
	SourceP99Ms     float64 `json:"source_p99_ms"`
	TargetP50Ms     float64 `json:"target_p50_ms"`
	TargetP99Ms     float64 `json:"target_p99_ms"`
	MismatchLog     string  `json:"mismatch_log"`
	GeneratedAt     string  `json:"generated_at"`
}

type latHist struct {
	mu   sync.Mutex
	vals []float64 // milliseconds
}

func (h *latHist) add(d time.Duration) {
	h.mu.Lock()
	h.vals = append(h.vals, float64(d.Microseconds())/1000)
	h.mu.Unlock()
}

func (h *latHist) percentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.vals) == 0 {
		return 0
	}
	cp := append([]float64(nil), h.vals...)
	sort.Float64s(cp)
	return cp[int(p/100*float64(len(cp)-1))]
}

// runContinuous loops KEYS sweeps until duration elapses or SIGINT arrives.
// A dead/hung target can NEVER crash or stall it: every Wavicle GET carries a
// hard deadline, failures are counters + log lines, and shutdown always writes
// the summary report first.
func runContinuous(src, dst, pattern string, count int, timeout, interval, duration, getTimeout time.Duration, logPath, reportPath string) {
	started := time.Now()
	var compared, mismatches, werrs, sweeps atomic.Int64
	srcHist, dstHist := &latHist{}, &latHist{}

	var logMu sync.Mutex
	var logFh *os.File
	if logPath != "" {
		var err error
		logFh, err = os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mismatch log: %v\n", err)
			os.Exit(1)
		}
		defer logFh.Close()
	}
	logMismatch := func(m mismatchLine) {
		if logFh == nil {
			fmt.Printf("MISMATCH key=%q src=%q dst=%q dstErr=%q\n", m.Key, m.Source, m.Target, m.TargetError)
			return
		}
		b, _ := json.Marshal(m)
		logMu.Lock()
		fmt.Fprintln(logFh, string(b))
		logMu.Unlock()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	deadline := time.Time{}
	if duration > 0 {
		deadline = started.Add(duration)
	}

	sweep := func() {
		keys, err := respKeys(src, pattern, timeout)
		if err != nil {
			fmt.Printf("sweep: source KEYS failed (%v) — skipping sweep, loop survives\n", err)
			return
		}
		sort.Strings(keys)
		if len(keys) > count {
			keys = keys[:count]
		}
		sweeps.Add(1)
		for _, k := range keys {
			a, aLat, errA := respGet(src, k, timeout, getTimeout)
			srcHist.add(aLat)
			b, bLat, errB := respGet(dst, k, timeout, getTimeout)
			dstHist.add(bLat)
			compared.Add(1)
			if errB != nil {
				werrs.Add(1)
			}
			if errA != nil || errB != nil || a != b {
				mismatches.Add(1)
				te := ""
				if errB != nil {
					te = errB.Error()
				}
				logMismatch(mismatchLine{
					Timestamp:   time.Now().UTC().Format(time.RFC3339),
					Key:         k,
					Source:      a,
					Target:      b,
					TargetError: te,
					SourceMs:    float64(aLat.Microseconds()) / 1000,
					TargetMs:    float64(bLat.Microseconds()) / 1000,
				})
			}
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
loop:
	for {
		sweep()
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
		select {
		case <-sigCh:
			fmt.Println("\nSIGINT — writing report")
			break loop
		case <-ticker.C:
		}
	}

	ct, mm := compared.Load(), mismatches.Load()
	rep := shadowReport{
		StartedAt:       started.UTC().Format(time.RFC3339),
		DurationS:       time.Since(started).Seconds(),
		Sweeps:          sweeps.Load(),
		ComparedTotal:   ct,
		MismatchTotal:   mm,
		MatchRatePct:    pct(int(ct), int(mm)),
		WavicleErrTotal: werrs.Load(),
		SourceP50Ms:     srcHist.percentile(50),
		SourceP99Ms:     srcHist.percentile(99),
		TargetP50Ms:     dstHist.percentile(50),
		TargetP99Ms:     dstHist.percentile(99),
		MismatchLog:     logPath,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if reportPath != "" {
		if err := os.WriteFile(reportPath, b, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "report write: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("report written to %s\n", reportPath)
	}
	if mm > 0 {
		os.Exit(2)
	}
}
