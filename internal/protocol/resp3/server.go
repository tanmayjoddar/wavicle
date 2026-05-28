package resp3

import (
	"bufio"
	"crypto/sha3"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"wavicle/internal/core"
	"wavicle/internal/engine"
	"wavicle/internal/fidelity"
	"wavicle/internal/storage"
)

type Server struct {
	crystal    *storage.CausalCrystal
	proofCache *engine.ProofCache
	policy     *fidelity.FidelityPolicy
}

func NewServer(crystal *storage.CausalCrystal) *Server {
	return &Server{
		crystal:    crystal,
		proofCache: engine.NewProofCache(64, 10000),
		policy:     &fidelity.DefaultPolicy,
	}
}

func (s *Server) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	for {
		args, err := readCommand(br)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
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
	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "PING":
		return "+PONG\r\n", nil

	case "SET":
		if len(args) < 3 {
			return "", fmt.Errorf("wrong number of arguments for 'set' command")
		}
		path := args[1]
		val := args[2]

		value := core.VString(val)
		expr := &core.EConst{Value: value}

		var causalPast []core.Hash
		if prev, ok := s.crystal.GetCurrent(path); ok {
			causalPast = []core.Hash{prev.Hash}
		}

		if _, err := s.crystal.AppendAtom(expr, path, causalPast); err != nil {
			return "", err
		}

		return "+OK\r\n", nil

	case "GET":
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'get' command")
		}
		path := args[1]

		h := sha3.New256()
		h.Write([]byte(path))
		h.Write([]byte{byte(core.ModeDeductive)})
		var queryHash core.Hash
		copy(queryHash[:], h.Sum(nil))

		current, ok := s.crystal.GetCurrent(path)
		if !ok {
			return "$-1\r\n", nil
		}
		// Return nil for tombstoned keys
		if current.Expr != nil {
			if e, ok := current.Expr.(*core.EConst); ok {
				if _, isNull := e.Value.(core.VNull); isNull {
					return "$-1\r\n", nil
				}
			}
		}

		if proof, ok := s.proofCache.Get(queryHash); ok {
			val, err := engine.ReduceIncremental(proof, s.crystal)
			if err == nil {
				return formatValue(val), nil
			}
		}

		proof, err := engine.ComposeProof(s.crystal, path, core.ModeDeductive)
		if err != nil {
			return "", err
		}
		s.proofCache.Set(queryHash, proof)

		return formatValue(proof.Value), nil

	case "DEL":
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'del' command")
		}
		deleted := 0
		for _, path := range args[1:] {
			if prev, ok := s.crystal.GetCurrent(path); ok {
				expr := &core.EConst{Value: core.VNull{}}
				causalPast := []core.Hash{prev.Hash}
				if _, err := s.crystal.AppendAtom(expr, path, causalPast); err == nil {
					deleted++
				}
			}
		}
		return fmt.Sprintf(":%d\r\n", deleted), nil

	case "EXISTS":
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'exists' command")
		}
		count := 0
		for _, path := range args[1:] {
			if atom, ok := s.crystal.GetCurrent(path); ok {
				if !isTombstone(atom) {
					count++
				}
			}
		}
		return fmt.Sprintf(":%d\r\n", count), nil

	case "DBSIZE":
		count := 0
		for _, path := range s.crystal.FrontierPaths() {
			if atom, ok := s.crystal.GetCurrent(path); ok && !isTombstone(atom) {
				count++
			}
		}
		return fmt.Sprintf(":%d\r\n", count), nil

	case "MGET":
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'mget' command")
		}
		paths := args[1:]
		resp := fmt.Sprintf("*%d\r\n", len(paths))
		for _, path := range paths {
			current, ok := s.crystal.GetCurrent(path)
			if !ok {
				resp += "$-1\r\n"
				continue
			}
			if isTombstone(current) {
				resp += "$-1\r\n"
				continue
			}
			h := sha3.New256()
			h.Write([]byte(path))
			h.Write([]byte{byte(core.ModeDeductive)})
			var queryHash core.Hash
			copy(queryHash[:], h.Sum(nil))

			if proof, ok := s.proofCache.Get(queryHash); ok {
				val, err := engine.ReduceIncremental(proof, s.crystal)
				if err == nil {
					resp += formatValue(val)
					continue
				}
			}
			proof, err := engine.ComposeProof(s.crystal, path, core.ModeDeductive)
			if err != nil {
				resp += "$-1\r\n"
				continue
			}
			s.proofCache.Set(queryHash, proof)
			resp += formatValue(proof.Value)
		}
		return resp, nil

	case "MSET":
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
			if prev, ok := s.crystal.GetCurrent(path); ok {
				causalPast = []core.Hash{prev.Hash}
			}
			if _, err := s.crystal.AppendAtom(expr, path, causalPast); err != nil {
				return "", err
			}
		}
		return "+OK\r\n", nil

	case "HSET":
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
		if prev, ok := s.crystal.GetCurrent(hashPath); ok {
			causalPast = []core.Hash{prev.Hash}
		}
		if _, err := s.crystal.AppendAtom(expr, hashPath, causalPast); err != nil {
			return "", err
		}
		return ":1\r\n", nil

	case "HGET":
		if len(args) < 3 {
			return "", fmt.Errorf("wrong number of arguments for 'hget' command")
		}
		key := args[1]
		field := args[2]
		hashPath := key + ":" + field
		current, ok := s.crystal.GetCurrent(hashPath)
		if !ok {
			return "$-1\r\n", nil
		}
		if isTombstone(current) {
			return "$-1\r\n", nil
		}
		h := sha3.New256()
		h.Write([]byte(hashPath))
		h.Write([]byte{byte(core.ModeDeductive)})
		var queryHash core.Hash
		copy(queryHash[:], h.Sum(nil))

		if proof, ok := s.proofCache.Get(queryHash); ok {
			val, err := engine.ReduceIncremental(proof, s.crystal)
			if err == nil {
				return formatValue(val), nil
			}
		}
		proof, err := engine.ComposeProof(s.crystal, hashPath, core.ModeDeductive)
		if err != nil {
			return "$-1\r\n", nil
		}
		s.proofCache.Set(queryHash, proof)
		return formatValue(proof.Value), nil

	case "HGETALL":
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'hgetall' command")
		}
		key := args[1]
		prefix := key + ":"
		var fields []string
		for _, path := range s.crystal.FrontierPaths() {
			if strings.HasPrefix(path, prefix) {
				if atom, ok := s.crystal.GetCurrent(path); ok && !isTombstone(atom) {
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
				val, err := engine.ReduceIncremental(proof, s.crystal)
				if err == nil {
					valStr = formatValue(val)
				}
			}
			if valStr == "" {
				proof, err := engine.ComposeProof(s.crystal, hashPath, core.ModeDeductive)
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
		current, ok := s.crystal.GetCurrent(path)
		if !ok || isTombstone(current) {
			return ":0\r\n", nil
		}
		// Append a new atom with the same value but mark for expiry
		// In production, TTL is handled by the DB adapter
		// In dev mode, we re-append the atom and the sweeper checks timestamps
		_ = seconds // Future: store in frontier expiresAt
		return ":1\r\n", nil

	case "TTL":
		if len(args) < 2 {
			return "", fmt.Errorf("wrong number of arguments for 'ttl' command")
		}
		current, ok := s.crystal.GetCurrent(args[1])
		if !ok {
			return ":-2\r\n", nil
		}
		if isTombstone(current) {
			return ":-2\r\n", nil
		}
		// Dev mode: no TTL tracking yet, return -1 (no expiry)
		return ":-1\r\n", nil

	default:
		return "", fmt.Errorf("unknown command '%s'", cmd)
	}
}

// formatValue converts a Wavicle Value to a RESP3 bulk string.
// If the value is a VRecord with a single entry, it extracts the inner value.
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
