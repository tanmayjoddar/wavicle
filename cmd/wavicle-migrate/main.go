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
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"time"
)

func main() {
	src := flag.String("source", "localhost:6379", "source Redis-compatible addr")
	dst := flag.String("target", "localhost:6380", "target Wavicle addr")
	pattern := flag.String("pattern", "*", "KEYS pattern")
	count := flag.Int("count", 10000, "max keys to compare")
	timeout := flag.Duration("timeout", 5*time.Second, "dial timeout")
	flag.Parse()

	keys, err := respKeys(*src, *pattern, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "source KEYS failed: %v\n", err)
		os.Exit(1)
	}
	sort.Strings(keys)
	if len(keys) > *count {
		keys = keys[:*count]
	}
	fmt.Printf("comparing %d keys pattern=%q\n", len(keys), *pattern)

	mismatch := 0
	checked := 0
	for _, k := range keys {
		a, errA := respGet(*src, k, *timeout)
		b, errB := respGet(*dst, k, *timeout)
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
	return c, bufio.NewReader(c), bufio.NewWriter(c), nil
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

func respGet(addr, key string, timeout time.Duration) (string, error) {
	c, r, w, err := dial(addr, timeout)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(timeout * 3))
	hdr, bulk, err := sendInline(w, r, "GET "+escapeArg(key))
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(hdr, "$-1") {
		return "(nil)", nil
	}
	if len(bulk) > 0 {
		return bulk[0], nil
	}
	return hdr, nil
}

func escapeArg(s string) string {
	if strings.ContainsAny(s, " \r\n") {
		return `"` + s + `"`
	}
	return s
}
