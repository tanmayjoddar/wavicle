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

		if _, ok := s.crystal.GetCurrent(path); !ok {
			return "$-1\r\n", nil
		}

		if proof, ok := s.proofCache.Get(queryHash); ok {
			val, err := engine.ReduceIncremental(proof, s.crystal)
			if err == nil {
				str := fmt.Sprintf("%v", val)
				return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str), nil
			}
		}

		proof, err := engine.ComposeProof(s.crystal, path, core.ModeDeductive)
		if err != nil {
			return "", err
		}
		s.proofCache.Set(queryHash, proof)

		str := fmt.Sprintf("%v", proof.Value)
		return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str), nil

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
			if _, ok := s.crystal.GetCurrent(path); ok {
				count++
			}
		}
		return fmt.Sprintf(":%d\r\n", count), nil

	case "DBSIZE":
		return fmt.Sprintf(":%d\r\n", len(s.crystal.FrontierPaths())), nil

	default:
		return "", fmt.Errorf("unknown command '%s'", cmd)
	}
}
