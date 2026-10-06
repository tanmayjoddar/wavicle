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
	"log"
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
	flag.DurationVar(interval, "sweep-interval", 30*time.Second, "alias of -interval")
	duration := flag.Duration("duration", 0*time.Second, "total run time in continuous mode (0 = until SIGINT)")
	getTimeout := flag.Duration("get-timeout", 50*time.Millisecond, "hard per-GET deadline; overruns count as Wavicle-side failures, never block the loop")
	flag.DurationVar(getTimeout, "op-timeout", 50*time.Millisecond, "alias of -get-timeout")
	sweepTimeout := flag.Duration("sweep-timeout", 90*time.Second, "hard per-sweep deadline; overruns abort the sweep, log sweep_timeout, continue looping")
	logPath := flag.String("log", "", "append JSON mismatch lines here (continuous mode)")
	flag.StringVar(logPath, "mismatch-log", "", "alias of -log")
	heartbeatPath := flag.String("heartbeat", "", "append 'alive sweep N t=Xs' per sweep here (proves liveness live, not at the end)")
	reportPath := flag.String("report", "shadow-report.json", "write summary report JSON here on exit (continuous mode)")
	flag.Parse()

	// Pilot stacks require AUTH; handshake once per connection when set.
	authPassword = *authPass

	if !*continuous {
		runOnce(*src, *dst, *pattern, *count, *timeout)
		return
	}
	runContinuous(*src, *dst, *pattern, *count, *timeout, *interval, *duration, *getTimeout, *sweepTimeout, *logPath, *heartbeatPath, *reportPath)
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

// pidLock is the on-disk record proving one comparer owns a report path.
type pidLock struct {
	PID       int    `json:"pid"`
	StartedAt string `json:"started_at"`
	Heartbeat string `json:"heartbeat"`
}

// staleLockAfter bounds heartbeat staleness: older means the owner is dead or
// wedged either way, and a loud takeover beats a silent shared report.
const staleLockAfter = 5 * time.Minute

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal 0 performs no delivery; error means no such live process.
	// PLATFORM HONESTY: on Windows this syscall reports "not supported",
	// i.e. always false — so it is only EVER a positive hint, never the
	// deciding vote. The heartbeat below is the binding rule on all OSes.
	if err := p.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	return true
}

func readLock(path string) (pidLock, bool) {
	var l pidLock
	b, err := os.ReadFile(path)
	if err != nil {
		return l, false
	}
	if err := json.Unmarshal(b, &l); err != nil {
		return pidLock{}, false
	}
	return l, true
}

func writeLock(path string, l pidLock) error {
	b, err := json.Marshal(l)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// acquireLock claims path or refuses loudly. Returns a release func removing
// the file on clean exit, plus a beat func refreshing it per sweep.
// THE RULE (same on every OS): a FRESH heartbeat refuses, period — liveness
// of the writer is proven by recency, not by PID games. A STALE heartbeat
// (older than staleLockAfter) takes over loudly, even if the PID looks alive
// (PID reuse makes "alive" untrustworthy on its own). The PID is recorded for
// human forensics (kill it yourself if you disagree with a takeover).
// Simultaneous starters are serialized by O_EXCL create: exactly one wins,
// the loser sees a brand-new file (no heartbeat yet = treat as fresh = refuse).
func acquireLock(path string) (release func(), beat func(), err error) {
	now := time.Now().UTC()
	own := pidLock{PID: os.Getpid(), StartedAt: now.Format(time.RFC3339), Heartbeat: now.Format(time.RFC3339)}
	ownBytes, _ := json.Marshal(own)
	fh, openErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if openErr == nil {
		// We won the race: sole owner.
		_, _ = fh.Write(ownBytes)
		_ = fh.Close()
		fmt.Printf("lock %s acquired by pid %d\n", path, own.PID)
	} else {
		// Someone was here first (or concurrently): read and judge. Retry
		// briefly — the winner may still be writing its first bytes, and a
		// half-read must refuse (safe direction), not clobber.
		var prev pidLock
		var ok bool
		for i := 0; i < 6; i++ {
			prev, ok = readLock(path)
			if ok {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		fresh := false
		if ok {
			if hb, perr := time.Parse(time.RFC3339, prev.Heartbeat); perr == nil {
				fresh = now.Sub(hb) < staleLockAfter
			} else {
				fresh = true // just-born file, no heartbeat yet — refuse, don't race it
			}
		} else {
			fresh = true // unreadable counts as fresh: refuse rather than clobber blindly
		}
		alive := ok && pidAlive(prev.PID)
		if fresh || alive {
			return nil, nil, fmt.Errorf(
				"another shadow comparer (pid %d, started %s, heartbeat %s) owns report lock %s. "+
					"Two writers corrupt one report — kill it or use a different -report path",
				prev.PID, prev.StartedAt, prev.Heartbeat, path)
		}
		fmt.Printf("TAKEOVER stale lock %s from pid %d (started %s, last heartbeat %s) — previous run crashed or was killed\n",
			path, prev.PID, prev.StartedAt, prev.Heartbeat)
		if err := writeLock(path, own); err != nil {
			return nil, nil, fmt.Errorf("cannot write lock %s: %w", path, err)
		}
		fmt.Printf("lock %s acquired by pid %d\n", path, own.PID)
	}
	var mu sync.Mutex
	beat = func() {
		mu.Lock()
		defer mu.Unlock()
		own.Heartbeat = time.Now().UTC().Format(time.RFC3339)
		_ = writeLock(path, own)
	}
	return func() { _ = os.Remove(path) }, beat, nil
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
	SweepTimeouts   int64   `json:"sweep_timeouts"`
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
// No failure mode stalls it: every Wavicle GET carries a hard deadline, each
// SWEEP carries a hard deadline (overruns abort with sweep_timeout, counted in
// the report), failures are counters + log lines, and shutdown always writes
// the summary report first. Every sweep logs one timing line, always — a hang
// is visible live in the log instead of discovered at the end.
func runContinuous(src, dst, pattern string, count int, timeout, interval, duration, getTimeout, sweepTimeout time.Duration, logPath, heartbeatPath, reportPath string) {
	// PID lock, tied to the report path: two comparers must never share one
	// report/JSONL pair (Sep-29 lesson — it halves apparent sweep cadence and
	// corrupts the report). Refuse loudly on a LIVE owner; take over — loudly —
	// a stale one (dead pid or heartbeat older than 5m).
	lockPath := reportPath + ".lock"
	if reportPath == "" {
		log.Fatal("continuous mode requires -report (PID lock is tied to it)")
	}
	release, beat, err := acquireLock(lockPath)
	if err != nil {
		log.Fatalf("REFUSING TO START: %v", err)
	}
	defer release()
	refreshLock := beat
	started := time.Now()
	var compared, mismatches, werrs, sweeps, sweepTimeouts atomic.Int64
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
	var hbMu sync.Mutex
	var hbFh *os.File
	if heartbeatPath != "" {
		var err error
		hbFh, err = os.OpenFile(heartbeatPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "heartbeat log: %v\n", err)
			os.Exit(1)
		}
		defer hbFh.Close()
	}
	heartbeat := func(n int64, took time.Duration, note string) {
		line := fmt.Sprintf("%s alive sweep=%d t=%.0fs %s\n",
			time.Now().UTC().Format(time.RFC3339), n, time.Since(started).Seconds(), note)
		fmt.Print(line)
		refreshLock()
		if hbFh == nil {
			return
		}
		hbMu.Lock()
		fmt.Fprint(hbFh, line)
		hbMu.Unlock()
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

	sweep := func() (timedOut bool) {
		n := sweeps.Add(1) // count ATTEMPTED sweeps first: a run that compares
		// nothing must report honestly, never a 100% match rate over zero
		// comparisons (lesson of the Sep-29 overnight run).
		t0 := time.Now()
		done := make(chan struct{})
		var keys []string
		var keysErr error
		var c, mm int
		go func() {
			defer close(done)
			var err error
			keys, err = respKeys(src, pattern, timeout)
			if err != nil {
				keysErr = err
				return
			}
			sort.Strings(keys)
			if len(keys) > count {
				keys = keys[:count]
			}
			for _, k := range keys {
				a, aLat, errA := respGet(src, k, timeout, getTimeout)
				srcHist.add(aLat)
				b, bLat, errB := respGet(dst, k, timeout, getTimeout)
				dstHist.add(bLat)
				compared.Add(1)
				c++
				if errB != nil {
					werrs.Add(1)
				}
				if errA != nil || errB != nil || a != b {
					mismatches.Add(1)
					mm++
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
		}()
		select {
		case <-done:
			took := time.Since(t0)
			heartbeat(n, took, fmt.Sprintf("keys=%d compared=%d mismatches=%d", len(keys), c, mm))
			if keysErr != nil {
				fmt.Printf("sweep %d: source KEYS failed (%v) — skipping sweep, loop survives\n", n, keysErr)
			}
			return false
		case <-time.After(sweepTimeout):
			// The abandoned goroutine still finishes on its own deadlines
			// (every GET is bounded) and its partial counters stand as-is.
			sweepTimeouts.Add(1)
			took := time.Since(t0)
			heartbeat(n, took, "SWEEP_TIMEOUT — aborted, counters kept, looping on")
			fmt.Printf("sweep %d: exceeded %v — aborted (sweep_timeout #%d), loop survives\n",
				n, sweepTimeout, sweepTimeouts.Load())
			return true
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
		SweepTimeouts:   sweepTimeouts.Load(),
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
