package resp3

import (
	"bufio"
	"context"
	"crypto/sha3"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/engine"
	"wavicle/internal/fidelity"
	"wavicle/internal/storage"
	"wavicle/internal/telemetry"
)

type Server struct {
	store       storage.Store
	proofCache  *engine.ProofCache
	policy      *fidelity.FidelityPolicy
	password    string
	maxConns    int
	activeConns atomic.Int32
	listener    net.Listener
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewServer(store storage.Store, password string, maxConns int) *Server {
	if maxConns <= 0 {
		maxConns = 10000
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		store:      store,
		proofCache: engine.NewProofCache(64, 2000),
		policy:     &fidelity.DefaultPolicy,
		password:   password,
		maxConns:   maxConns,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (s *Server) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = l
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return nil // Shutting down gracefully
			default:
				continue
			}
		}

		if s.activeConns.Load() >= int32(s.maxConns) {
			conn.Write([]byte("-ERR max number of clients reached\r\n"))
			conn.Close()
			continue
		}

		s.activeConns.Add(1)
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) Close() {
	s.cancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer s.activeConns.Add(-1)
	defer telemetry.Get().DecActiveConnections()

	telemetry.Get().IncActiveConnections()

	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	authenticated := s.password == ""

	for {
		args, err := readCommand(br)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		// Handle AUTH separately
		if cmd == "AUTH" {
			if len(args) < 2 {
				bw.WriteString("-ERR wrong number of arguments for 'auth' command\r\n")
			} else if s.password == "" {
				bw.WriteString("-ERR Client sent AUTH, but no password is set\r\n")
			} else if args[1] == s.password {
				authenticated = true
				bw.WriteString("+OK\r\n")
			} else {
				bw.WriteString("-ERR invalid password\r\n")
			}
			bw.Flush()
			continue
		}

		if !authenticated {
			// Only PING is allowed unauthenticated in some Redis-like configs, 
			// but we'll be strict: only AUTH or PING.
			if cmd != "PING" {
				bw.WriteString("-NOAUTH Authentication required.\r\n")
				bw.Flush()
				continue
			}
		}

		resp, err := s.HandleCommand(args)
		if err != nil {
			bw.WriteString(fmt.Sprintf("-ERR %v\r\n", err))
		} else {
			bw.WriteString(resp)
		}
		bw.Flush()
	}
}

const maxArgs = 10000
const maxArgLen = 1 << 20

func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	if len(line) == 0 {
		return nil, nil
	}

	if line[0] == '*' {
		count, err := strconv.Atoi(line[1:])
		if err != nil || count <= 0 || count > maxArgs {
			return nil, fmt.Errorf("invalid array length: %s", line[1:])
		}
		args := make([]string, 0, count)
		for i := 0; i < count; i++ {
			bulk, err := r.ReadString('\n')
			if err != nil {
				return nil, err
			}
			bulk = strings.TrimRight(bulk, "\r\n")
			if len(bulk) == 0 || bulk[0] != '$' {
				return nil, fmt.Errorf("expected bulk string at arg %d", i)
			}
			strLen, err := strconv.Atoi(bulk[1:])
			if err != nil || strLen < -1 || strLen > maxArgLen {
				return nil, fmt.Errorf("invalid bulk string length at arg %d: %s", i, bulk[1:])
			}
			if strLen == -1 {
				args = append(args, "")
				continue
			}
			data := make([]byte, strLen+2)
			if _, err := io.ReadFull(r, data); err != nil {
				return nil, err
			}
			args = append(args, string(data[:strLen]))
		}
		return args, nil
	}

	fields := strings.Fields(line)
	if len(fields) > maxArgs {
		return nil, fmt.Errorf("too many arguments: %d > %d", len(fields), maxArgs)
	}
	return fields, nil
}

func (s *Server) HandleCommand(args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("empty command")
	}
	start := time.Now()
	cmd := strings.ToUpper(args[0])
	defer func() {
		telemetry.Get().RecordDuration(cmd, time.Since(start))
	}()

	switch cmd {
	case "PING":
		telemetry.Get().RecordRequest("PING")
		return "+PONG\r\n", nil

	case "SET":
		telemetry.Get().RecordRequest("SET")
		if len(args) < 3 {
			return "", fmt.Errorf("wrong number of arguments for 'set' command")
		}
		path := args[1]
		val := args[2]

		value := core.VString(val)
		expr := core.NewEConst(value)

		var causalPast []core.Hash
		if prev, ok := s.store.GetCurrent(path); ok {
			causalPast = []core.Hash{prev.Hash}
		}

		if _, err := s.store.AppendAtom(expr, path, causalPast, time.Time{}); err != nil {
			telemetry.Get().RecordError()
			return "", err
		}

		return "+OK\r\n", nil

	case "GET":
		telemetry.Get().RecordRequest("GET")
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'get' command")
		}
		path := args[1]

		// Support for transparent SQL query caching
		if strings.HasPrefix(strings.ToUpper(path), "SELECT") {
			expr, qPath, err := engine.SQLToProofTree(path, s.store)
			if err == nil {
				path = qPath
				// Check if we already have this composed proof
				h := sha3.New256()
				h.Write([]byte(path))
				h.Write([]byte{byte(core.ModeDeductive)})
				var queryHash core.Hash
				copy(queryHash[:], h.Sum(nil))

				if proof, ok := s.proofCache.Get(queryHash); ok {
					val, err := engine.ReduceIncremental(proof, s.store)
					if err == nil {
						telemetry.Get().RecordCacheHit()
						return formatValue(val), nil
					}
				}

				// If not in cache or stale, reduce the new expr
				val, cache, err := engine.ReduceProofTree(expr, s.store)
				if err == nil {
					telemetry.Get().RecordProofReduction()
					_ = cache // Future: populate nodeCache in MaterializedProof
					return formatValue(val), nil
				}
			}
		}

		h := sha3.New256()
		h.Write([]byte(path))
		h.Write([]byte{byte(core.ModeDeductive)})
		var queryHash core.Hash
		copy(queryHash[:], h.Sum(nil))

		current, ok := s.store.GetCurrent(path)
		if !ok {
			telemetry.Get().RecordCacheMiss()
			return "$-1\r\n", nil
		}
		// Return nil for tombstoned keys
		if isTombstone(current) {
			telemetry.Get().RecordCacheMiss()
			return "$-1\r\n", nil
		}

		if proof, ok := s.proofCache.Get(queryHash); ok {
			val, err := engine.ReduceIncremental(proof, s.store)
			if err == nil {
				telemetry.Get().RecordCacheHit()
				telemetry.Get().RecordIncrementalHit()
				return formatValue(val), nil
			}
		}

		telemetry.Get().RecordCacheMiss()
		telemetry.Get().RecordProofReduction()
		proof, err := engine.ComposeProof(s.store, path, core.ModeDeductive)
		if err != nil {
			telemetry.Get().RecordError()
			return "", err
		}
		s.proofCache.Set(queryHash, proof)

		return formatValue(proof.Value), nil

	case "DEL":
		telemetry.Get().RecordRequest("DEL")
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'del' command")
		}
		deleted := 0
		for _, path := range args[1:] {
			if prev, ok := s.store.GetCurrent(path); ok {
				expr := &core.EConst{Value: core.VNull{}}
				causalPast := []core.Hash{prev.Hash}
				if _, err := s.store.AppendAtom(expr, path, causalPast, time.Time{}); err == nil {
					deleted++
				}
			}
		}
		return fmt.Sprintf(":%d\r\n", deleted), nil

	case "EXISTS":
		telemetry.Get().RecordRequest("EXISTS")
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'exists' command")
		}
		count := 0
		for _, path := range args[1:] {
			if atom, ok := s.store.GetCurrent(path); ok {
				if !isTombstone(atom) {
					count++
				}
			}
		}
		return fmt.Sprintf(":%d\r\n", count), nil

	case "DBSIZE":
		telemetry.Get().RecordRequest("DBSIZE")
		count := 0
		for _, path := range s.store.FrontierPaths() {
			if atom, ok := s.store.GetCurrent(path); ok && !isTombstone(atom) {
				count++
			}
		}
		return fmt.Sprintf(":%d\r\n", count), nil

	case "MGET":
		telemetry.Get().RecordRequest("MGET")
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'mget' command")
		}
		paths := args[1:]
		resp := fmt.Sprintf("*%d\r\n", len(paths))
		for _, path := range paths {
			current, ok := s.store.GetCurrent(path)
			if !ok || isTombstone(current) {
				resp += "$-1\r\n"
				continue
			}
			h := sha3.New256()
			h.Write([]byte(path))
			h.Write([]byte{byte(core.ModeDeductive)})
			var queryHash core.Hash
			copy(queryHash[:], h.Sum(nil))

			if proof, ok := s.proofCache.Get(queryHash); ok {
				val, err := engine.ReduceIncremental(proof, s.store)
				if err == nil {
					telemetry.Get().RecordCacheHit()
					resp += formatValue(val)
					continue
				}
			}
			proof, err := engine.ComposeProof(s.store, path, core.ModeDeductive)
			if err != nil {
				resp += "$-1\r\n"
				continue
			}
			s.proofCache.Set(queryHash, proof)
			resp += formatValue(proof.Value)
		}
		return resp, nil

	case "MSET":
		telemetry.Get().RecordRequest("MSET")
		if len(args) < 3 || len(args[1:])%2 != 0 {
			return "", fmt.Errorf("wrong number of arguments for 'mset' command")
		}
		pairs := args[1:]
		for i := 0; i < len(pairs); i += 2 {
			path := pairs[i]
			val := pairs[i+1]
			value := core.VString(val)
			expr := &core.EConst{Value: value}
			var causalPast []core.Hash
			if prev, ok := s.store.GetCurrent(path); ok {
				causalPast = []core.Hash{prev.Hash}
			}
			if _, err := s.store.AppendAtom(expr, path, causalPast, time.Time{}); err != nil {
				telemetry.Get().RecordError()
				return "", err
			}
		}
		return "+OK\r\n", nil

	case "HSET":
		telemetry.Get().RecordRequest("HSET")
		if len(args) < 4 {
			return "", fmt.Errorf("wrong number of arguments for 'hset' command")
		}
		key := args[1]
		field := args[2]
		val := args[3]
		hashPath := key + ":" + field
		value := core.VString(val)
		expr := &core.EConst{Value: value}
		var causalPast []core.Hash
		if prev, ok := s.store.GetCurrent(hashPath); ok {
			causalPast = []core.Hash{prev.Hash}
		}
		if _, err := s.store.AppendAtom(expr, hashPath, causalPast, time.Time{}); err != nil {
			telemetry.Get().RecordError()
			return "", err
		}
		return ":1\r\n", nil

	case "HGET":
		telemetry.Get().RecordRequest("HGET")
		if len(args) < 3 {
			return "", fmt.Errorf("wrong number of arguments for 'hget' command")
		}
		key := args[1]
		field := args[2]
		hashPath := key + ":" + field
		current, ok := s.store.GetCurrent(hashPath)
		if !ok || isTombstone(current) {
			return "$-1\r\n", nil
		}
		h := sha3.New256()
		h.Write([]byte(hashPath))
		h.Write([]byte{byte(core.ModeDeductive)})
		var queryHash core.Hash
		copy(queryHash[:], h.Sum(nil))

		if proof, ok := s.proofCache.Get(queryHash); ok {
			val, err := engine.ReduceIncremental(proof, s.store)
			if err == nil {
				telemetry.Get().RecordCacheHit()
				return formatValue(val), nil
			}
		}
		proof, err := engine.ComposeProof(s.store, hashPath, core.ModeDeductive)
		if err != nil {
			return "$-1\r\n", nil
		}
		s.proofCache.Set(queryHash, proof)
		return formatValue(proof.Value), nil

	case "HGETALL":
		telemetry.Get().RecordRequest("HGETALL")
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'hgetall' command")
		}
		key := args[1]
		prefix := key + ":"
		var fields []string
		for _, path := range s.store.FrontierPaths() {
			if strings.HasPrefix(path, prefix) {
				if atom, ok := s.store.GetCurrent(path); ok && !isTombstone(atom) {
					fieldName := path[len(prefix):]
					fields = append(fields, fieldName)
				}
			}
		}
		// Build response as an array of alternating field-value pairs
		resp := fmt.Sprintf("*%d\r\n", len(fields)*2)
		for _, f := range fields {
			hashPath := prefix + f
			h := sha3.New256()
			h.Write([]byte(hashPath))
			h.Write([]byte{byte(core.ModeDeductive)})
			var queryHash core.Hash
			copy(queryHash[:], h.Sum(nil))

			var valStr string
			if proof, ok := s.proofCache.Get(queryHash); ok {
				val, err := engine.ReduceIncremental(proof, s.store)
				if err == nil {
					telemetry.Get().RecordCacheHit()
					valStr = formatValue(val)
				}
			}
			if valStr == "" {
				proof, err := engine.ComposeProof(s.store, hashPath, core.ModeDeductive)
				if err == nil {
					s.proofCache.Set(queryHash, proof)
					valStr = formatValue(proof.Value)
				}
			}
			resp += bulkString(f)
			if valStr != "" {
				resp += valStr
			} else {
				resp += "$-1\r\n"
			}
		}
		return resp, nil

	case "EXPIRE":
		telemetry.Get().RecordRequest("EXPIRE")
		if len(args) < 3 {
			return "", fmt.Errorf("wrong number of arguments for 'expire' command")
		}
		path := args[1]
		seconds, err := strconv.Atoi(args[2])
		if err != nil {
			return "", fmt.Errorf("invalid expire time")
		}
		if seconds <= 0 {
			return ":0\r\n", nil
		}
		current, ok := s.store.GetCurrent(path)
		if !ok || isTombstone(current) {
			return ":0\r\n", nil
		}
		
		// Append a new atom with the same value but updated ExpiresAt
		expiresAt := time.Now().Add(time.Duration(seconds) * time.Second)
		if _, err := s.store.AppendAtom(current.Expr, path, []core.Hash{current.Hash}, expiresAt); err != nil {
			telemetry.Get().RecordError()
			return "", err
		}
		return ":1\r\n", nil

	case "TTL":
		telemetry.Get().RecordRequest("TTL")
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'ttl' command")
		}
		current, ok := s.store.GetCurrent(args[1])
		if !ok || isTombstone(current) {
			return ":-2\r\n", nil
		}
		
		if current.ExpiresAt.IsZero() {
			return ":-1\r\n", nil
		}
		ttl := time.Until(current.ExpiresAt).Seconds()
		if ttl <= 0 {
			return ":-2\r\n", nil
		}
		return fmt.Sprintf(":%d\r\n", int(ttl)), nil

	default:
		return "", fmt.Errorf("unknown command '%s'", cmd)
	}
}

func formatValue(v core.Value) string {
	if rec, ok := v.(core.VRecord); ok && len(rec) == 1 {
		for _, val := range rec {
			return formatBulkString(fmt.Sprintf("%v", val))
		}
	}
	return formatBulkString(fmt.Sprintf("%v", v))
}

func formatBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func isTombstone(atom *core.CausalAtom) bool {
	if atom == nil || atom.Expr == nil {
		return false
	}
	if e, ok := atom.Expr.(*core.EConst); ok {
		_, isNull := e.Value.(core.VNull)
		return isNull
	}
	return false
}

func bulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
