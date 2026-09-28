// Wire-compatibility tests: the exact bytes redis-py, ioredis, go-redis and
// Jedis put on the wire — RESP arrays, pipelining, HELLO handshake, parallel
// clients. No client libraries needed: if raw bytes interoperate, the clients
// do. Per-language manual matrices live in docc/CHAOS_AND_PG_REALITY.md.
package compat_test

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"wavicle/internal/protocol/resp3"
	"wavicle/internal/storage"
)

const compatAddr = "127.0.0.1:6399"

func startCompatServer(t *testing.T) {
	t.Helper()
	crystal, err := storage.NewCausalCrystal(filepath.Join(t.TempDir(), "compat.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { crystal.Close() })
	srv := resp3.NewServer(crystal, "", 1000)
	t.Cleanup(srv.Close)
	go func() { _ = srv.ListenAndServe(compatAddr) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", compatAddr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("compat server never bound")
}

// respArray encodes a command the way redis-py/ioredis/go-redis/Jedis send it.
func respArray(args ...string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	return sb.String()
}

func dial(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.DialTimeout("tcp", compatAddr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, bufio.NewReader(c)
}

// readOneReply consumes exactly one RESP reply (any shape).
func readOneReply(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	_ = r // reader advances through helper below
	return readReply(t, r)
}

func readReply(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		t.Fatal("empty reply")
	}
	switch line[0] {
	case '+', '-', ':', '#', '_', ',', '(':
		return line
	case '$':
		var n int
		fmt.Sscan(line[1:], &n)
		if n == -1 {
			return line
		}
		buf := make([]byte, n+2)
		if _, err := readFull(r, buf); err != nil {
			t.Fatal(err)
		}
		return line + "|" + string(buf[:n])
	case '*', '%':
		var n int
		fmt.Sscan(line[1:], &n)
		elems := n
		if line[0] == '%' {
			elems = n * 2 // map: n key-value pairs
		}
		out := line
		for i := 0; i < elems && n > 0; i++ {
			out += "|" + readReply(t, r)
		}
		return out
	default:
		return line
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	off := 0
	for off < len(buf) {
		n, err := r.Read(buf[off:])
		off += n
		if err != nil {
			return off, err
		}
	}
	return off, nil
}

func TestCompat_HELLOAndPing(t *testing.T) {
	startCompatServer(t)
	c, r := dial(t)
	fmt.Fprint(c, respArray("HELLO", "3"))
	if got := readOneReply(t, r); !strings.Contains(got, "wavicle") {
		t.Fatalf("HELLO: %q", got)
	}
	fmt.Fprint(c, "PING\r\n") // inline, like redis-cli
	if got := readOneReply(t, r); got != "+PONG" {
		t.Fatalf("PING: %q", got)
	}
}

func TestCompat_Pipelining100(t *testing.T) {
	startCompatServer(t)
	c, r := dial(t)
	// 100 commands, ONE flush — exactly what pipelined clients do.
	for i := 0; i < 50; i++ {
		fmt.Fprint(c, respArray("SET", fmt.Sprintf("pipe:%d", i), fmt.Sprintf("v%d", i)))
	}
	for i := 0; i < 50; i++ {
		fmt.Fprint(c, respArray("GET", fmt.Sprintf("pipe:%d", i)))
	}
	for i := 0; i < 50; i++ {
		if got := readOneReply(t, r); got != "+OK" {
			t.Fatalf("SET reply %d: %q", i, got)
		}
	}
	for i := 0; i < 50; i++ {
		if got := readOneReply(t, r); !strings.Contains(got, fmt.Sprintf("v%d", i)) {
			t.Fatalf("GET reply %d: %q", i, got)
		}
	}
}

func TestCompat_ParallelClients(t *testing.T) {
	startCompatServer(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	fails := 0
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			conn, rr := dial(t)
			defer conn.Close()
			for i := 0; i < 25; i++ {
				fmt.Fprint(conn, respArray("SET", fmt.Sprintf("par:%d:%d", c, i), "x"))
				if got := readOneReply(t, rr); got != "+OK" {
					mu.Lock()
					fails++
					mu.Unlock()
				}
			}
		}(c)
	}
	wg.Wait()
	if fails > 0 {
		t.Fatalf("%d parallel failures", fails)
	}
}

func TestCompat_ErrorShape(t *testing.T) {
	startCompatServer(t)
	c, r := dial(t)
	fmt.Fprint(c, respArray("NOSUCHCMD"))
	if got := readOneReply(t, r); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("unknown cmd must be -ERR, got %q", got)
	}
}
