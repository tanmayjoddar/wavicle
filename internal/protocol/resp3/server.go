package resp3

import (
	"bufio"
	"context"
	"crypto/sha3"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"wavicle/internal/auth"
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
	acl         *auth.ACL
	tlsConfig   *tls.Config
	maxConns    int
	activeConns atomic.Int32
	listener    net.Listener
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	// keyTypes is the production type index (Redis robj equivalent).
	// Maps logical key -> "string|list|set|zset|hash|stream".
	// Authoritative on write path (every write sets it); best-effort on
	// read path (cold restart / CDC-written keys fall back to shape-sniff
	// and re-populate). Never the sole correctness gate: guards always
	// re-verify the stored value shape before refusing with WRONGTYPE.
	keyTypes sync.Map // string -> string
	// Stale guard v2 (fail-closed reads). The oracle is CDC HEALTH, not event
	// recency — that distinction is the whole design (see below).
	//
	// PG: slotState shared with the slot monitor: -1 unknown (boot grace,
	// serve), -2 slot broken (fail), >=0 retained-WAL lag bytes (fail at
	// slotPageBytes). An idle-but-healthy database holds lag ≈ 0, so quiet
	// streams SERVE; only proven behind-or-blind streams fail reads.
	// MySQL: lastOK is the poller's last fully-clean tick (0 = never, grace);
	// a quiet-but-healthy database polls clean, so idle SERVES there too.
	// Neither set = disabled (dev mode, fail-open — today's default).
	stalePGState *atomic.Int64
	stalePGPage  int64
	staleMyLast  *atomic.Int64
	staleMyMax   time.Duration
}

// SetStaleGuardPG arms fail-closed reads from slot health (see field docs).
func (s *Server) SetStaleGuardPG(slotState *atomic.Int64, pageBytes int64) {
	s.stalePGState = slotState
	s.stalePGPage = pageBytes
}

// SetStaleGuardMySQL arms fail-closed reads from poll health.
func (s *Server) SetStaleGuardMySQL(lastOK *atomic.Int64, maxSilence time.Duration) {
	s.staleMyLast = lastOK
	s.staleMyMax = maxSilence
}

// staleRefused returns non-nil when a data read must fail closed.
func (s *Server) staleRefused() error {
	if s.stalePGState != nil {
		switch st := s.stalePGState.Load(); {
		case st == -2:
			return fmt.Errorf("STALE CDC blind (slot missing/inactive) — check listener and slot")
		case st >= 0 && st >= s.stalePGPage:
			return fmt.Errorf("STALE slot lag %d bytes at page threshold %d — check CDC listener", st, s.stalePGPage)
		}
	}
	if s.staleMyLast != nil && s.staleMyMax > 0 {
		if last := s.staleMyLast.Load(); last != 0 {
			if lag := time.Since(time.Unix(0, last)); lag > s.staleMyMax {
				return fmt.Errorf("STALE MySQL poller silent %v over max %v — check poller and DB", lag.Round(time.Millisecond), s.staleMyMax)
			}
		}
	}
	return nil
}

var serverStartTime = time.Now()

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
		acl:        auth.NewSinglePassword(password),
		maxConns:   maxConns,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// SetACL replaces the auth gate (production: multi-user ACL file).
func (s *Server) SetACL(a *auth.ACL) { s.acl = a }

// SetTLS enables TLS termination on the next ListenAndServe call.
func (s *Server) SetTLS(cfg *tls.Config) { s.tlsConfig = cfg }

func (s *Server) ListenAndServe(addr string) error {
	var l net.Listener
	var err error
	if s.tlsConfig != nil {
		l, err = tls.Listen("tcp", addr, s.tlsConfig)
	} else {
		l, err = net.Listen("tcp", addr)
	}
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

	authenticated := s.password == "" && (s.acl == nil || !s.acl.Enabled())
	username := "default"

	var inMulti bool
	var queue [][]string

	for {
		args, err := readCommand(br)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		// Transaction framing: MULTI queues, EXEC runs atomically (single-threaded
		// apply under store locks), DISCARD drops.
		if cmd == "MULTI" {
			if inMulti {
				bw.WriteString("-ERR MULTI calls can not be nested\r\n")
				bw.Flush()
				continue
			}
			inMulti = true
			queue = nil
			bw.WriteString("+OK\r\n")
			bw.Flush()
			continue
		}
		if cmd == "DISCARD" {
			if !inMulti {
				bw.WriteString("-ERR DISCARD without MULTI\r\n")
				bw.Flush()
				continue
			}
			inMulti = false
			queue = nil
			bw.WriteString("+OK\r\n")
			bw.Flush()
			continue
		}
		if cmd == "EXEC" {
			if !inMulti {
				bw.WriteString("-ERR EXEC without MULTI\r\n")
				bw.Flush()
				continue
			}
			inMulti = false
			if len(queue) == 0 {
				bw.WriteString("*0\r\n")
				bw.Flush()
				continue
			}
			bw.WriteString(fmt.Sprintf("*%d\r\n", len(queue)))
			for _, q := range queue {
				// FIX: EXEC must enforce the same ACL as live commands.
				// Queued commands were checked at queue time only for syntax;
				// re-check here in case policy changed mid-transaction.
				if s.acl != nil && s.acl.Enabled() && authenticated {
					denied := false
					for _, k := range allKeys(q) {
						if err := s.acl.Authorize(username, q[0], k); err != nil {
							bw.WriteString(fmt.Sprintf("-ERR %v\r\n", err))
							denied = true
							break
						}
					}
					if denied {
						continue
					}
				}
				r, e := s.HandleCommand(q)
				if e != nil {
					bw.WriteString(fmt.Sprintf("-ERR %v\r\n", e))
				} else {
					bw.WriteString(r)
				}
			}
			bw.Flush()
			queue = nil
			continue
		}
		if inMulti {
			// Queue everything except connection control.
			// FIX: fail fast on ACL at queue time (Redis behavior).
			if s.acl != nil && s.acl.Enabled() && authenticated {
				failed := false
				for _, k := range allKeys(args) {
					if err := s.acl.Authorize(username, args[0], k); err != nil {
						bw.WriteString(fmt.Sprintf("-ERR %v\r\n", err))
						failed = true
						break
					}
				}
				if failed {
					bw.Flush()
					continue
				}
			}
			queue = append(queue, append([]string(nil), args...))
			bw.WriteString("+QUEUED\r\n")
			bw.Flush()
			continue
		}

		// Handle AUTH separately (supports AUTH password and AUTH user password)
		if cmd == "AUTH" {
			user, pass := "default", ""
			if len(args) == 2 {
				pass = args[1]
			} else if len(args) >= 3 {
				user, pass = args[1], args[2]
			} else {
				bw.WriteString("-ERR wrong number of arguments for 'auth' command\r\n")
				bw.Flush()
				continue
			}
			ok := false
			if s.acl != nil && s.acl.Enabled() {
				ok = s.acl.Authenticate(user, pass)
			} else if s.password == "" || pass == s.password {
				ok = true
			}
			if ok {
				authenticated = true
				username = user
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

		// ACL authorize (command + every key — FIX: MSET/MGET/DEL carry
		// multiple keys; checking only the first leaks access to the rest).
		if s.acl != nil && s.acl.Enabled() && authenticated {
			denied := false
			for _, k := range allKeys(args) {
				if err := s.acl.Authorize(username, cmd, k); err != nil {
					bw.WriteString(fmt.Sprintf("-ERR %v\r\n", err))
					bw.Flush()
					denied = true
					break
				}
			}
			if denied {
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

		if err := s.assertKeyType(path, "string"); err != nil {
			return "", err
		}
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
		s.setKeyType(path, "string")

		return "+OK\r\n", nil

	case "GET":
		telemetry.Get().RecordRequest("GET")
		if err := s.staleRefused(); err != nil {
			return "", err
		}
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
					s.delKeyType(path)
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
		if err := s.staleRefused(); err != nil {
			return "", err
		}
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
			if err := s.assertKeyType(path, "string"); err != nil {
				return "", err
			}
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
			s.setKeyType(path, "string")
		}
		return "+OK\r\n", nil

	case "HSET":
		telemetry.Get().RecordRequest("HSET")
		if len(args) < 4 {
			return "", fmt.Errorf("wrong number of arguments for 'hset' command")
		}
		key := args[1]
		if err := s.assertKeyType(key, "hash"); err != nil {
			if s.keyTypeOf(key) != "none" {
				return "", err
			}
		}
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
		s.setKeyType(key, "hash")
		return ":1\r\n", nil

	case "HGET":
		telemetry.Get().RecordRequest("HGET")
		if err := s.staleRefused(); err != nil {
			return "", err
		}
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
		if err := s.staleRefused(); err != nil {
			return "", err
		}
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
		// Fail-closed guard covers extended reads too (they serve stored
		// values that CDC may have left behind). Pure writes skip it —
		// writes can't serve stale data, and blocking them would turn a
		// stale-reader problem into a full outage.
		if isExtendedRead(cmd) {
			if err := s.staleRefused(); err != nil {
				return "", err
			}
		}
		if resp, ok := s.HandleExtended(args); ok {
			return resp, nil
		}
		return "", fmt.Errorf("unknown command '%s'", cmd)
	}
}

// isExtendedRead reports whether an extended command serves stored data
// (including mutating reads like LPOP, which serve before mutating).
func isExtendedRead(cmd string) bool {
	switch cmd {
	case "STRLEN", "LLEN", "LRANGE", "LINDEX", "LPOP", "RPOP",
		"SMEMBERS", "SCARD", "SISMEMBER", "ZSCORE", "ZCARD", "ZRANGE",
		"HMGET", "HLEN", "HEXISTS", "HKEYS", "HVALS",
		"XLEN", "XRANGE", "KEYS", "SCAN":
		return true
	}
	return false
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

// firstKey extracts the first key arg for ACL checks.
// Covers KV/hash/list/set/zset/stream + MSET/MGET multi-key (returns first).
func firstKey(args []string) string {
	if len(args) < 2 {
		return ""
	}
	switch strings.ToUpper(args[0]) {
	case "PING", "DBSIZE", "INFO", "HELLO", "CLIENT", "CONFIG", "FLUSHDB", "FLUSHALL",
		"SCRIPT", "FUNCTION", "MULTI", "EXEC", "DISCARD", "SCAN", "KEYS":
		return ""
	case "MGET", "DEL", "EXISTS":
		return args[1]
	case "MSET":
		return args[1]
	default:
		return args[1]
	}
}

// allKeys extracts every key a command touches for ACL checks.
// FIX: MGET/DEL/EXISTS/HMGET/HDEL/SREM/ZREM carry N keys; MSET/HMSET carry
// pairs; checking only args[1] leaked access to the rest.
func allKeys(args []string) []string {
	if len(args) < 2 {
		return nil
	}
	switch strings.ToUpper(args[0]) {
	case "PING", "DBSIZE", "INFO", "HELLO", "CLIENT", "CONFIG", "FLUSHDB", "FLUSHALL",
		"SCRIPT", "FUNCTION", "MULTI", "EXEC", "DISCARD", "SCAN", "KEYS":
		return nil
	case "MGET", "DEL", "EXISTS":
		return args[1:]
	case "MSET":
		var out []string
		for i := 1; i < len(args); i += 2 {
			out = append(out, args[i])
		}
		return out
	case "HMGET", "HDEL", "SREM", "ZREM":
		// key + fields: only the key is a keyspace gate; fields are not keys.
		return args[1:2]
	default:
		return args[1:2]
	}
}

// ---- production type index (keyTypes) ----

func (s *Server) setKeyType(key, typ string) { s.keyTypes.Store(key, typ) }

func (s *Server) delKeyType(key string) { s.keyTypes.Delete(key) }

func (s *Server) clearKeyTypes() {
	s.keyTypes.Range(func(k, _ any) bool { s.keyTypes.Delete(k); return true })
}

// keyTypeOf returns the indexed type, falling back to value-shape inference
// (cold index, CDC-written keys) and re-populating the index on hit.
func (s *Server) keyTypeOf(key string) string {
	if v, ok := s.keyTypes.Load(key); ok {
		if t, ok := v.(string); ok {
			return t
		}
	}
	// Fallback: infer from stored value.
	val, _, ok := func() (core.Value, *core.CausalAtom, bool) {
		atom, ok := s.store.GetCurrent(key)
		if !ok || isTombstone(atom) {
			return nil, nil, false
		}
		if ec, ok := atom.Expr.(*core.EConst); ok {
			return ec.Value, atom, true
		}
		return nil, atom, true
	}()
	if !ok {
		// Maybe a hash parent (key:field children, no direct value).
		prefix := key + ":"
		for _, p := range s.store.FrontierPaths() {
			if len(p) > len(prefix) && p[:len(prefix)] == prefix {
				s.keyTypes.Store(key, "hash")
				return "hash"
			}
		}
		return "none"
	}
	var t string
	switch v := val.(type) {
	case core.VArray:
		t = "list"
		// Streams are VArray of VRecord with _id; refine.
		if len(v) > 0 {
			if rec, ok := v[0].(core.VRecord); ok {
				if _, hasID := rec["_id"]; hasID {
					t = "stream"
				}
			}
		}
	case core.VRecord:
		if isZSetRecord(val) {
			t = "zset"
		} else if isSetRecord(val) {
			t = "set"
		} else {
			t = "hash"
		}
	default:
		t = "string"
	}
	s.keyTypes.Store(key, t)
	return t
}

// assertKeyType enforces Redis type semantics using index first, value second.
// Returns nil if key is absent (caller creates) or types match.
func (s *Server) assertKeyType(key, want string) error {
	if t := s.keyTypeOf(key); t != "none" && t != want {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return nil
}
