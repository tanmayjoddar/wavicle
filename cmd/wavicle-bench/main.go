// Command wavicle-bench: raw-RESP load generator with latency histograms.
//
// Speaks RESP over TCP with stdlib only, so the SAME binary benchmarks Wavicle,
// real Redis, or Dragonfly — run it twice and diff. Reports throughput plus
// p50/p99 per operation. Two scenarios:
//
//	readwrite (default): mixed SET/GET over N keys, C clients, pipelined.
//	visibility:         external-writer analog — a second connection SETs a key
//	                     while readers poll it; measures time-to-visibility
//	                     p50/p99 (the stale-window number TTL cannot beat).
//
// Example: compare Wavicle vs Redis on identical workload:
//
//	wavicle-bench -target localhost:6379 -mode readwrite -clients 16 -duration 30s
//	wavicle-bench -target redis:6379    -mode readwrite -clients 16 -duration 30s
//	wavicle-bench -target localhost:6379 -mode visibility -duration 30s
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	target   = flag.String("target", "localhost:6379", "host:port of server under test")
	mode     = flag.String("mode", "readwrite", "readwrite | visibility")
	clients  = flag.Int("clients", 16, "parallel connections (readwrite)")
	keys     = flag.Int("keys", 1000, "key pool size")
	duration = flag.Duration("duration", 30*time.Second, "run length")
	pipeline = flag.Int("pipeline", 10, "commands per flush (readwrite)")
	auth     = flag.String("auth", "", "password (sent as AUTH once per conn)")
)

type conn struct {
	c  net.Conn
	br *bufio.Reader
	bw *bufio.Writer
}

func dial() (*conn, error) {
	c, err := net.DialTimeout("tcp", *target, 5*time.Second)
	if err != nil {
		return nil, err
	}
	k := &conn{c: c, br: bufio.NewReaderSize(c, 1<<20), bw: bufio.NewWriterSize(c, 1<<20)}
	if *auth != "" {
		if _, err := k.roundTrip("AUTH " + *auth); err != nil {
			c.Close()
			return nil, err
		}
	}
	return k, nil
}

// roundTrip sends one inline command, reads one reply.
func (k *conn) roundTrip(line string) (string, error) {
	if _, err := fmt.Fprintf(k.bw, "%s\r\n", line); err != nil {
		return "", err
	}
	if err := k.bw.Flush(); err != nil {
		return "", err
	}
	return readReply(k.br)
}

func readReply(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", io.ErrUnexpectedEOF
	}
	switch line[0] {
	case '+', '-', ':', '%', '#', '_', ',', '(':
		return line, nil
	case '$':
		var n int
		fmt.Sscan(line[1:], &n)
		if n == -1 {
			return line, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return line + "|" + string(buf[:n]), nil
	case '*':
		var n int
		fmt.Sscan(line[1:], &n)
		out := line
		for i := 0; i < n && n > 0; i++ {
			hdr, err := r.ReadString('\n')
			if err != nil {
				return "", err
			}
			hdr = strings.TrimRight(hdr, "\r\n")
			out += "|" + hdr
			if strings.HasPrefix(hdr, "$") {
				var m int
				fmt.Sscan(hdr[1:], &m)
				if m > 0 {
					buf := make([]byte, m+2)
					if _, err := io.ReadFull(r, buf); err != nil {
						return "", err
					}
					out += "=" + string(buf[:m])
				}
			}
		}
		return out, nil
	default:
		return line, nil
	}
}

type hist struct {
	mu   sync.Mutex
	vals []time.Duration
}

func (h *hist) add(d time.Duration) {
	h.mu.Lock()
	h.vals = append(h.vals, d)
	h.mu.Unlock()
}

func (h *hist) percentile(p float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.vals) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), h.vals...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp[int(p/100*float64(len(cp)-1))]
}

func (h *hist) n() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.vals)
}

// perOp spreads a pipelined batch over its commands with float division —
// integer Duration division truncates sub-10ns means to a lying "0s".
func perOp(t0 time.Time) time.Duration {
	return time.Duration(float64(time.Since(t0)) / float64(*pipeline))
}

func main() {
	flag.Parse()
	switch *mode {
	case "readwrite":
		runReadWrite()
	case "visibility":
		runVisibility()
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(1)
	}
}

func runReadWrite() {
	stop := make(chan struct{})
	var opsGet, opsSet, errs atomic.Int64
	hGet, hSet := &hist{}, &hist{}
	var wg sync.WaitGroup
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			k, err := dial()
			if err != nil {
				fmt.Fprintf(os.Stderr, "dial: %v\n", err)
				return
			}
			defer k.c.Close()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Pipeline: batch SETs then batch GETs per flush.
				for p := 0; p < *pipeline; p++ {
					key := fmt.Sprintf("bench:%d", (c*1000000+i*7919+p)%*keys)
					fmt.Fprintf(k.bw, "SET %s v%d\r\n", key, i)
				}
				k.bw.Flush()
				t0 := time.Now()
				for p := 0; p < *pipeline; p++ {
					if _, err := readReply(k.br); err != nil {
						errs.Add(1)
					} else {
						opsSet.Add(1)
					}
				}
				hSet.add(perOp(t0))
				for p := 0; p < *pipeline; p++ {
					key := fmt.Sprintf("bench:%d", (c*1000000+i*7919+p)%*keys)
					fmt.Fprintf(k.bw, "GET %s\r\n", key)
				}
				k.bw.Flush()
				t0 = time.Now()
				for p := 0; p < *pipeline; p++ {
					if _, err := readReply(k.br); err != nil {
						errs.Add(1)
					} else {
						opsGet.Add(1)
					}
				}
				hGet.add(perOp(t0))
				i++
			}
		}(c)
	}
	time.Sleep(*duration)
	close(stop)
	wg.Wait()
	total := opsGet.Load() + opsSet.Load()
	fmt.Printf("target=%s mode=readwrite clients=%d keys=%d pipeline=%d duration=%v\n",
		*target, *clients, *keys, *pipeline, *duration)
	fmt.Printf("ops=%d (SET %d, GET %d) errors=%d throughput=%.0f ops/s\n",
		total, opsSet.Load(), opsGet.Load(), errs.Load(), float64(total)/duration.Seconds())
	fmt.Printf("SET p50=%v p99=%v | GET p50=%v p99=%v\n",
		hSet.percentile(50), hSet.percentile(99), hGet.percentile(50), hGet.percentile(99))
}

func runVisibility() {
	// One writer connection flips a key; one reader polls until it observes
	// each flip. Samples = full round-trip visibility lag per generation.
	w, err := dial()
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer w.c.Close()
	r, err := dial()
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer r.c.Close()

	h := &hist{}
	deadline := time.Now().Add(*duration)
	gen := 0
	for time.Now().Before(deadline) {
		gen++
		val := fmt.Sprintf("gen-%d", gen)
		t0 := time.Now()
		if _, err := w.roundTrip("SET vis:key " + val); err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			continue
		}
		// Poll until observed (10ms budget each — beyond that IS the story).
		for {
			got, err := r.roundTrip("GET vis:key")
			if err != nil {
				break
			}
			if strings.Contains(got, val) {
				h.add(time.Since(t0))
				break
			}
			if time.Since(t0) > 10*time.Second {
				h.add(time.Since(t0))
				break
			}
		}
	}
	fmt.Printf("target=%s mode=visibility samples=%d p50=%v p99=%v\n",
		*target, h.n(), h.percentile(50), h.percentile(99))
	fmt.Printf("NOTE: same-process visibility ~= RTT. True external-writer lag needs " +
		"a real DB writer (see consistency PG test); TTL systems add their TTL on top.\n")
}
