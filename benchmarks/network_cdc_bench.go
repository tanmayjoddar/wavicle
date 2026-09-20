// Package benchmarks provides load testing and latency benchmarking across network boundaries.
package benchmarks

import (
	"bufio"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// NetworkBenchConfig holds parameters for remote network latency benchmarking.
type NetworkBenchConfig struct {
	PostgresDSN string
	WavicleAddr string
	Concurrency int
	TotalOps    int
}

// NetworkBenchResult reports real latency distributions across a network.
type NetworkBenchResult struct {
	TotalOps       int
	Duration       time.Duration
	OpsPerSec      float64
	CDCLagP50Ms    float64
	CDCLagP95Ms    float64
	CDCLagP99Ms    float64
	ReadLatencyP50 time.Duration
	ReadLatencyP95 time.Duration
	ReadLatencyP99 time.Duration
	Errors         int
}

// RunNetworkBenchmark executes a distributed benchmark against a remote Postgres + Wavicle setup.
func RunNetworkBenchmark(cfg NetworkBenchConfig) (*NetworkBenchResult, error) {
	if cfg.PostgresDSN == "" {
		cfg.PostgresDSN = os.Getenv("REMOTE_PG_DSN")
	}
	if cfg.WavicleAddr == "" {
		cfg.WavicleAddr = os.Getenv("REMOTE_WAVICLE_ADDR")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 20
	}
	if cfg.TotalOps <= 0 {
		cfg.TotalOps = 10000
	}

	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}

	// 1. Measure CDC Invalidation Latency across network
	var cdcLags []float64
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("net_bench_%d", i)
		val := fmt.Sprintf("val_%d_%d", i, time.Now().UnixNano())

		// Ensure table row exists
		_, _ = db.Exec("INSERT INTO users (id, name, email) VALUES ($1, $2, 'bench@wavicle.io') ON CONFLICT (id) DO UPDATE SET name = $2", key, val)

		// Connect to remote Wavicle
		conn, err := net.Dial("tcp", cfg.WavicleAddr)
		if err != nil {
			return nil, fmt.Errorf("wavicle dial: %w", err)
		}

		// Mutate in Postgres and measure time until remote Wavicle reflects it
		mutatedVal := fmt.Sprintf("mutated_%d_%d", i, time.Now().UnixNano())
		start := time.Now()
		_, err = db.Exec("UPDATE users SET name = $1 WHERE id = $2", mutatedVal, key)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("postgres update: %w", err)
		}

		deadline := time.Now().Add(5 * time.Second)
		detected := false
		for time.Now().Before(deadline) {
			got := queryRESP3(conn, fmt.Sprintf("GET users:%s:name", key))
			if got == mutatedVal {
				cdcLags = append(cdcLags, float64(time.Since(start).Milliseconds()))
				detected = true
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		conn.Close()

		if !detected {
			return nil, fmt.Errorf("CDC propagation timed out on key %s", key)
		}
	}

	sort.Float64s(cdcLags)

	// 2. High-concurrency read soak test over network sockets
	var latencies []time.Duration
	var latMu sync.Mutex
	var errCount int
	var errMu sync.Mutex

	opsPerWorker := cfg.TotalOps / cfg.Concurrency
	var wg sync.WaitGroup
	benchStart := time.Now()

	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", cfg.WavicleAddr)
			if err != nil {
				errMu.Lock()
				errCount++
				errMu.Unlock()
				return
			}
			defer conn.Close()

			localLats := make([]time.Duration, 0, opsPerWorker)
			reader := bufio.NewReader(conn)

			for op := 0; op < opsPerWorker; op++ {
				key := fmt.Sprintf("users:net_bench_%d:name\r\n", op%20)
				cmd := "GET " + key

				t0 := time.Now()
				if _, err := conn.Write([]byte(cmd)); err != nil {
					errMu.Lock()
					errCount++
					errMu.Unlock()
					return
				}

				line, err := reader.ReadString('\n')
				if err != nil {
					errMu.Lock()
					errCount++
					errMu.Unlock()
					return
				}
				if strings.HasPrefix(line, "$") && line != "$-1\r\n" {
					_, _ = reader.ReadString('\n')
				}
				localLats = append(localLats, time.Since(t0))
			}

			latMu.Lock()
			latencies = append(latencies, localLats...)
			latMu.Unlock()
		}(w)
	}

	wg.Wait()
	benchDuration := time.Since(benchStart)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	res := &NetworkBenchResult{
		TotalOps:  len(latencies),
		Duration:  benchDuration,
		OpsPerSec: float64(len(latencies)) / benchDuration.Seconds(),
		Errors:    errCount,
	}

	if len(cdcLags) > 0 {
		res.CDCLagP50Ms = cdcLags[len(cdcLags)*50/100]
		res.CDCLagP95Ms = cdcLags[len(cdcLags)*95/100]
		res.CDCLagP99Ms = cdcLags[len(cdcLags)*99/100]
	}

	if len(latencies) > 0 {
		res.ReadLatencyP50 = latencies[len(latencies)*50/100]
		res.ReadLatencyP95 = latencies[len(latencies)*95/100]
		res.ReadLatencyP99 = latencies[len(latencies)*99/100]
	}

	return res, nil
}

func queryRESP3(conn net.Conn, cmd string) string {
	fmt.Fprintf(conn, "%s\r\n", cmd)
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return ""
	}
	line = strings.TrimRight(line, "\r\n")
	if strings.HasPrefix(line, "$") && line != "$-1" {
		payload, err := reader.ReadString('\n')
		if err != nil {
			return ""
		}
		return strings.TrimRight(payload, "\r\n")
	}
	return line
}
