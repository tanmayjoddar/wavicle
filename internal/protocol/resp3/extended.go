package resp3

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"wavicle/internal/core"
	"wavicle/internal/telemetry"
)

// Extended Redis 7 subset: strings/counters, lists, sets, zsets, hashes,
// generic, transactions (via connection-level buffering in server.go),
// streams (minimal). All state lives in the Store as EConst values so the
// proof engine's version-vector freshness still applies on GET paths.

// ---- value helpers ----

func curValue(s *Server, path string) (core.Value, *core.CausalAtom, bool) {
	atom, ok := s.store.GetCurrent(path)
	if !ok || isTombstone(atom) {
		return nil, nil, false
	}
	if ec, ok := atom.Expr.(*core.EConst); ok {
		return ec.Value, atom, true
	}
	return nil, atom, true
}

func putValue(s *Server, path string, v core.Value) error {
	_, err := s.store.AppendAtom(core.NewEConst(v), path, nil, time.Time{})
	return err
}

// wrongTypeResp is the canonical RESP error for type violations.
// Production note: wrongType() keeps its (string, error) shape only because
// 20+ call sites use the `r, _ := wrongType()` pattern. New code should use
// wrongTypeResp directly with `return wrongTypeResp, true`.
const wrongTypeResp = "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

func wrongType() (string, error) {
	return wrongTypeResp, fmt.Errorf("%s", "WRONGTYPE Operation against a key holding the wrong kind of value")
}

func toString(v core.Value) (string, bool) {
	switch t := v.(type) {
	case core.VString:
		return string(t), true
	case core.VInt:
		return strconv.FormatInt(int64(t), 10), true
	case core.VFloat:
		return strconv.FormatFloat(float64(t), 'f', -1, 64), true
	case core.VBool:
		if bool(t) {
			return "1", true
		}
		return "0", true
	default:
		return "", false
	}
}

// FIX: sets (VInt(1) values) and zsets (VFloat values) share VRecord storage.
// Without discrimination SADD over a ZSET silently merged. These guards enforce Redis WRONGTYPE.
func isSetRecord(v core.Value) bool {
	m, ok := v.(core.VRecord)
	if !ok {
		return false
	}
	for _, sv := range m {
		iv, ok := sv.(core.VInt)
		if !ok || iv != 1 {
			return false
		}
	}
	return true
}

func isZSetRecord(v core.Value) bool {
	m, ok := v.(core.VRecord)
	if !ok || len(m) == 0 {
		return false
	}
	for _, sv := range m {
		if _, ok := sv.(core.VFloat); !ok {
			return false
		}
	}
	return true
}

// HandleExtended handles commands beyond the core KV+Hash set.
// Returns (resp, true) if handled, ("", false) if unknown.
func (s *Server) HandleExtended(args []string) (string, bool) {
	if len(args) == 0 {
		return "", false
	}
	cmd := strings.ToUpper(args[0])
	switch cmd {

	// ---- counters ----
	case "INCR", "DECR", "INCRBY", "DECRBY":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		telemetry.Get().RecordRequest(cmd)
		delta := int64(1)
		if cmd == "DECR" {
			delta = -1
		}
		if (cmd == "INCRBY" || cmd == "DECRBY") && len(args) >= 3 {
			d, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return "ERR value is not an integer\r\n", true
			}
			delta = d
			if cmd == "DECRBY" {
				delta = -delta
			}
		}
		var cur int64
		if err := s.assertKeyType(args[1], "string"); err != nil {
			r, _ := wrongType()
			return r, true
		}
		if v, _, ok := curValue(s, args[1]); ok {
			str, isStr := toString(v)
			if !isStr {
				r, _ := wrongType()
				return r, true
			}
			c, err := strconv.ParseInt(strings.TrimSpace(str), 10, 64)
			if err != nil {
				return "ERR value is not an integer or out of range\r\n", true
			}
			cur = c
		}
		cur += delta
		_ = putValue(s, args[1], core.VString(strconv.FormatInt(cur, 10)))
		s.setKeyType(args[1], "string")
		return fmt.Sprintf(":%d\r\n", cur), true

	case "APPEND":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		telemetry.Get().RecordRequest(cmd)
		if err := s.assertKeyType(args[1], "string"); err != nil {
			r, _ := wrongType()
			return r, true
		}
		cur := ""
		if v, _, ok := curValue(s, args[1]); ok {
			str, isStr := toString(v)
			if !isStr {
				r, _ := wrongType()
				return r, true
			}
			cur = str
		}
		cur += args[2]
		_ = putValue(s, args[1], core.VString(cur))
		s.setKeyType(args[1], "string")
		return fmt.Sprintf(":%d\r\n", len(cur)), true

	case "STRLEN":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		if v, _, ok := curValue(s, args[1]); ok {
			str, isStr := toString(v)
			if !isStr {
				r, _ := wrongType()
				return r, true
			}
			return fmt.Sprintf(":%d\r\n", len(str)), true
		}
		return ":0\r\n", true

	case "GETSET":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		if err := s.assertKeyType(args[1], "string"); err != nil {
			r, _ := wrongType()
			return r, true
		}
		old := "$-1\r\n"
		if v, _, ok := curValue(s, args[1]); ok {
			if str, isStr := toString(v); isStr {
				old = formatBulkString(str)
			}
		}
		_ = putValue(s, args[1], core.VString(args[2]))
		s.setKeyType(args[1], "string")
		return old, true

	case "SETNX", "SETEX", "PSETEX":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		telemetry.Get().RecordRequest(cmd)
		if cmd == "SETNX" {
			if _, _, ok := curValue(s, args[1]); ok {
				return ":0\r\n", true
			}
			_ = putValue(s, args[1], core.VString(args[2]))
			s.setKeyType(args[1], "string")
			return ":1\r\n", true
		}
		// SETEX key seconds value / PSETEX key ms value
		if len(args) < 4 {
			return "ERR wrong number of arguments\r\n", true
		}
		var exp time.Time
		if cmd == "SETEX" {
			sec, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil || sec <= 0 {
				return "ERR invalid expire time\r\n", true
			}
			exp = time.Now().Add(time.Duration(sec) * time.Second)
			_ = func() error { _, err := s.store.AppendAtom(core.NewEConst(core.VString(args[3])), args[1], nil, exp); return err }()
			s.setKeyType(args[1], "string")
		} else {
			ms, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil || ms <= 0 {
				return "ERR invalid expire time\r\n", true
			}
			exp = time.Now().Add(time.Duration(ms) * time.Millisecond)
			_, _ = s.store.AppendAtom(core.NewEConst(core.VString(args[3])), args[1], nil, exp)
			s.setKeyType(args[1], "string")
		}
		return "+OK\r\n", true

	// ---- lists (stored as VArray of VString) ----
	case "LPUSH", "RPUSH":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		telemetry.Get().RecordRequest(cmd)
		if err := s.assertKeyType(args[1], "list"); err != nil {
			r, _ := wrongType()
			return r, true
		}
		var arr core.VArray
		if v, _, ok := curValue(s, args[1]); ok {
			a, isArr := v.(core.VArray)
			if !isArr {
				r, _ := wrongType()
				return r, true
			}
			arr = a
		}
		elems := args[2:]
		if cmd == "LPUSH" {
			// Redis LPUSH pushes left-to-right so last arg ends leftmost.
			for _, e := range elems {
				arr = append(core.VArray{core.VString(e)}, arr...)
			}
		} else {
			for _, e := range elems {
				arr = append(arr, core.VString(e))
			}
		}
		_ = putValue(s, args[1], arr)
		s.setKeyType(args[1], "list")
		return fmt.Sprintf(":%d\r\n", len(arr)), true

	case "LPOP", "RPOP":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		if err := s.assertKeyType(args[1], "list"); err != nil {
			r, _ := wrongType()
			return r, true
		}
		count := 1
		if len(args) >= 3 {
			c, err := strconv.Atoi(args[2])
			if err != nil || c <= 0 {
				return "ERR value is not an integer or out of range\r\n", true
			}
			count = c
		}
		v, _, ok := curValue(s, args[1])
		if !ok {
			return "$-1\r\n", true
		}
		arr, isArr := v.(core.VArray)
		if !isArr {
			r, _ := wrongType()
			return r, true
		}
		if len(arr) == 0 {
			return "$-1\r\n", true
		}
		if count > len(arr) {
			count = len(arr)
		}
		var popped []core.Value
		var rest core.VArray
		if cmd == "LPOP" {
			popped, rest = arr[:count], arr[count:]
		} else {
			// FIX: Redis RPOP count returns in pop order (rightmost first).
			// arr=[a b c d], RPOP 2 -> [d c], not [c d].
			tail := arr[len(arr)-count:]
			popped = make([]core.Value, 0, count)
			for i := len(tail) - 1; i >= 0; i-- {
				popped = append(popped, tail[i])
			}
			rest = arr[:len(arr)-count]
		}
		if len(rest) == 0 {
			_, _ = s.store.AppendAtom(&core.EConst{Value: core.VNull{}}, args[1], nil, time.Time{})
			s.delKeyType(args[1])
		} else {
			_ = putValue(s, args[1], rest)
		}
		if count == 1 {
			if str, ok := toString(popped[0]); ok {
				return formatBulkString(str), true
			}
			return formatBulkString(fmt.Sprint(popped[0])), true
		}
		resp := fmt.Sprintf("*%d\r\n", len(popped))
		for _, p := range popped {
			if str, ok := toString(p); ok {
				resp += formatBulkString(str)
			} else {
				resp += formatBulkString(fmt.Sprint(p))
			}
		}
		return resp, true

	case "LLEN":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		if v, _, ok := curValue(s, args[1]); ok {
			arr, isArr := v.(core.VArray)
			if !isArr {
				r, _ := wrongType()
				return r, true
			}
			return fmt.Sprintf(":%d\r\n", len(arr)), true
		}
		return ":0\r\n", true

	case "LRANGE":
		if len(args) < 4 {
			return "ERR wrong number of arguments\r\n", true
		}
		v, _, ok := curValue(s, args[1])
		if !ok {
			return "*0\r\n", true
		}
		arr, isArr := v.(core.VArray)
		if !isArr {
			r, _ := wrongType()
			return r, true
		}
		start, err1 := strconv.Atoi(args[2])
		stop, err2 := strconv.Atoi(args[3])
		if err1 != nil || err2 != nil {
			return "ERR value is not an integer or out of range\r\n", true
		}
		n := len(arr)
		if start < 0 {
			start = n + start
		}
		if stop < 0 {
			stop = n + stop
		}
		if start < 0 {
			start = 0
		}
		if stop >= n {
			stop = n - 1
		}
		if start >= n || start > stop {
			return "*0\r\n", true
		}
		slice := arr[start : stop+1]
		resp := fmt.Sprintf("*%d\r\n", len(slice))
		for _, e := range slice {
			if str, ok := toString(e); ok {
				resp += formatBulkString(str)
			} else {
				resp += formatBulkString(fmt.Sprint(e))
			}
		}
		return resp, true

	case "LINDEX":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		v, _, ok := curValue(s, args[1])
		if !ok {
			return "$-1\r\n", true
		}
		arr, isArr := v.(core.VArray)
		if !isArr {
			r, _ := wrongType()
			return r, true
		}
		idx, err := strconv.Atoi(args[2])
		if err != nil {
			return "ERR value is not an integer or out of range\r\n", true
		}
		if idx < 0 {
			idx = len(arr) + idx
		}
		if idx < 0 || idx >= len(arr) {
			return "$-1\r\n", true
		}
		if str, ok := toString(arr[idx]); ok {
			return formatBulkString(str), true
		}
		return formatBulkString(fmt.Sprint(arr[idx])), true

	// ---- sets (stored as VRecord member->VInt(1)) ----
	case "SADD":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		telemetry.Get().RecordRequest(cmd)
		if err := s.assertKeyType(args[1], "set"); err != nil {
			r, _ := wrongType()
			return r, true
		}
		set := core.VRecord{}
		if v, _, ok := curValue(s, args[1]); ok {
			// FIX: reject ZSETs (VFloat values) and lists with WRONGTYPE.
			if !isSetRecord(v) {
				if _, isRec := v.(core.VRecord); isRec {
					r, _ := wrongType() // zset or hash-shaped record
					return r, true
				}
				r, _ := wrongType()
				return r, true
			}
			for k, val := range v.(core.VRecord) {
				set[k] = val
			}
		}
		added := 0
		for _, m := range args[2:] {
			if _, exists := set[m]; !exists {
				added++
			}
			set[m] = core.VInt(1)
		}
		_ = putValue(s, args[1], set)
		s.setKeyType(args[1], "set")
		return fmt.Sprintf(":%d\r\n", added), true

	case "SREM":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		if err := s.assertKeyType(args[1], "set"); err != nil {
			r, _ := wrongType()
			return r, true
		}
		v, _, ok := curValue(s, args[1])
		if !ok {
			return ":0\r\n", true
		}
		if !isSetRecord(v) {
			r, _ := wrongType()
			return r, true
		}
		set := v.(core.VRecord)
		removed := 0
		for _, m := range args[2:] {
			if _, exists := set[m]; exists {
				delete(set, m)
				removed++
			}
		}
		if len(set) == 0 {
			_, _ = s.store.AppendAtom(&core.EConst{Value: core.VNull{}}, args[1], nil, time.Time{})
			s.delKeyType(args[1])
		} else {
			_ = putValue(s, args[1], set)
		}
		return fmt.Sprintf(":%d\r\n", removed), true

	case "SMEMBERS":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		v, _, ok := curValue(s, args[1])
		if !ok {
			return "*0\r\n", true
		}
		if !isSetRecord(v) {
			r, _ := wrongType()
			return r, true
		}
		set := v.(core.VRecord)
		members := make([]string, 0, len(set))
		for k := range set {
			members = append(members, k)
		}
		sort.Strings(members)
		resp := fmt.Sprintf("*%d\r\n", len(members))
		for _, m := range members {
			resp += bulkString(m)
		}
		return resp, true

	case "SCARD":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		if v, _, ok := curValue(s, args[1]); ok {
			if !isSetRecord(v) {
				r, _ := wrongType()
				return r, true
			}
			return fmt.Sprintf(":%d\r\n", len(v.(core.VRecord))), true
		}
		return ":0\r\n", true

	case "SISMEMBER":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		if v, _, ok := curValue(s, args[1]); ok {
			if !isSetRecord(v) {
				r, _ := wrongType()
				return r, true
			}
			if _, exists := v.(core.VRecord)[args[2]]; exists {
				return ":1\r\n", true
			}
		}
		return ":0\r\n", true

	// ---- zsets (stored as VRecord member->VFloat score) ----
	case "ZADD":
		if len(args) < 4 || (len(args)-2)%2 != 0 {
			return "ERR wrong number of arguments\r\n", true
		}
		telemetry.Get().RecordRequest(cmd)
		if err := s.assertKeyType(args[1], "zset"); err != nil {
			// Production: index is authoritative, but an empty record is
			// ambiguous (fresh key vs SET) — allow empty through to shape check.
			if s.keyTypeOf(args[1]) != "none" {
				r, _ := wrongType()
				return r, true
			}
		}
		z := core.VRecord{}
		if v, _, ok := curValue(s, args[1]); ok {
			// FIX: empty record is ambiguous (fresh ZADD vs SET); allow empty.
			// Non-empty must be all-VFloat, else WRONGTYPE (set or string).
			if m, isRec := v.(core.VRecord); !isRec {
				r, _ := wrongType()
				return r, true
			} else if len(m) > 0 && !isZSetRecord(v) {
				r, _ := wrongType()
				return r, true
			} else {
				for k, val := range m {
					z[k] = val
				}
			}
		}
		added := 0
		for i := 2; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(args[i], 64)
			if err != nil {
				return "ERR value is not a valid float\r\n", true
			}
			member := args[i+1]
			if _, exists := z[member]; !exists {
				added++
			}
			z[member] = core.VFloat(score)
		}
		_ = putValue(s, args[1], z)
		s.setKeyType(args[1], "zset")
		return fmt.Sprintf(":%d\r\n", added), true

	case "ZSCORE":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		if v, _, ok := curValue(s, args[1]); ok {
			if !isZSetRecord(v) {
				r, _ := wrongType()
				return r, true
			}
			z := v.(core.VRecord)
			if sc, exists := z[args[2]]; exists {
				if f, ok := sc.(core.VFloat); ok {
					return formatBulkString(strconv.FormatFloat(float64(f), 'f', -1, 64)), true
				}
			}
		}
		return "$-1\r\n", true

	case "ZCARD":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		if v, _, ok := curValue(s, args[1]); ok {
			if !isZSetRecord(v) {
				r, _ := wrongType()
				return r, true
			}
			z := v.(core.VRecord)
			return fmt.Sprintf(":%d\r\n", len(z)), true
		}
		return ":0\r\n", true

	case "ZREM":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		v, _, ok := curValue(s, args[1])
		if !ok {
			return ":0\r\n", true
		}
		if !isZSetRecord(v) {
			r, _ := wrongType()
			return r, true
		}
		z := v.(core.VRecord)
		removed := 0
		for _, m := range args[2:] {
			if _, exists := z[m]; exists {
				delete(z, m)
				removed++
			}
		}
		if len(z) == 0 {
			_, _ = s.store.AppendAtom(&core.EConst{Value: core.VNull{}}, args[1], nil, time.Time{})
			s.delKeyType(args[1])
		} else {
			_ = putValue(s, args[1], z)
		}
		return fmt.Sprintf(":%d\r\n", removed), true

	case "ZRANGE":
		if len(args) < 4 {
			return "ERR wrong number of arguments\r\n", true
		}
		withScores := len(args) > 4 && strings.ToUpper(args[4]) == "WITHSCORES"
		v, _, ok := curValue(s, args[1])
		if !ok {
			return "*0\r\n", true
		}
		if !isZSetRecord(v) {
			r, _ := wrongType()
			return r, true
		}
		z := v.(core.VRecord)
		type ms struct {
			m string
			s float64
		}
		var all []ms
		for m, sc := range z {
			f, _ := sc.(core.VFloat)
			all = append(all, ms{m, float64(f)})
		}
		sort.Slice(all, func(i, j int) bool {
			if all[i].s == all[j].s {
				return all[i].m < all[j].m
			}
			return all[i].s < all[j].s
		})
		start, err1 := strconv.Atoi(args[2])
		stop, err2 := strconv.Atoi(args[3])
		if err1 != nil || err2 != nil {
			return "ERR value is not an integer or out of range\r\n", true
		}
		n := len(all)
		if start < 0 {
			start = n + start
		}
		if stop < 0 {
			stop = n + stop
		}
		if start < 0 {
			start = 0
		}
		if stop >= n {
			stop = n - 1
		}
		if start >= n || start > stop {
			return "*0\r\n", true
		}
		slice := all[start : stop+1]
		if withScores {
			resp := fmt.Sprintf("*%d\r\n", len(slice)*2)
			for _, e := range slice {
				resp += bulkString(e.m)
				resp += bulkString(strconv.FormatFloat(e.s, 'f', -1, 64))
			}
			return resp, true
		}
		resp := fmt.Sprintf("*%d\r\n", len(slice))
		for _, e := range slice {
			resp += bulkString(e.m)
		}
		return resp, true

	// ---- hash extras (hashes are key:field paths in FrontierCache) ----
	case "HMSET", "HMGET", "HDEL", "HLEN", "HEXISTS", "HKEYS", "HVALS":
		return s.handleHashExtended(args)

	// ---- generic ----
	case "TYPE":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		v, _, ok := curValue(s, args[1])
		if !ok {
			// Maybe a hash-prefix key?
			prefix := args[1] + ":"
			for _, p := range s.store.FrontierPaths() {
				if strings.HasPrefix(p, prefix) {
					return "+hash\r\n", true
				}
			}
			return "+none\r\n", true
		}
		switch v.(type) {
		case core.VString, core.VInt, core.VFloat, core.VBool, core.VBytes:
			return "+string\r\n", true
		case core.VArray:
			return "+list\r\n", true
		case core.VRecord:
			return "+hash\r\n", true
		default:
			return "+string\r\n", true
		}

	case "KEYS":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		pat := args[1]
		// Support prefix* and * only (no full glob engine).
		var prefix string
		matchAll := pat == "*" || pat == ""
		if !matchAll {
			if strings.HasSuffix(pat, "*") {
				prefix = strings.TrimSuffix(pat, "*")
			} else {
				prefix = pat
			}
		}
		seen := map[string]bool{}
		var keys []string
		allPaths := s.store.FrontierPaths()
		for _, p := range allPaths {
			// Production rule: collapse p to its parent ONLY when the parent
			// is a real hash (explicit index — HSET/HMSET tagged it).
			// Flat colon keys (shadow:1, users:42:name) pass through intact.
			// The old direct-set heuristic collapsed those too (caught live
			// by the shadow runner: 200 shadow:N keys listed as one "shadow").
			key := p
			if idx := strings.LastIndex(p, ":"); idx > 0 {
				if s.isHashKey(p[:idx]) {
					key = p[:idx]
				}
			}
			if matchAll || strings.HasPrefix(key, prefix) || strings.HasPrefix(p, prefix) {
				seen[key] = true
			}
		}
		for k := range seen {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		resp := fmt.Sprintf("*%d\r\n", len(keys))
		for _, k := range keys {
			resp += bulkString(k)
		}
		return resp, true

	case "SCAN":
		// SCAN cursor [MATCH pat] [COUNT n] — stateless single pass.
		cursor := 0
		pat := "*"
		count := 100
		for i := 1; i < len(args); i++ {
			u := strings.ToUpper(args[i])
			if u == "MATCH" && i+1 < len(args) {
				pat = args[i+1]
				i++
			} else if u == "COUNT" && i+1 < len(args) {
				if c, err := strconv.Atoi(args[i+1]); err == nil && c > 0 {
					count = c
				}
				i++
			} else if i == 1 {
				if c, err := strconv.Atoi(args[i]); err == nil {
					cursor = c
				}
			}
		}
		_ = cursor
		prefix := ""
		if pat != "*" && pat != "" {
			prefix = strings.TrimSuffix(pat, "*")
		}
		seenScan := map[string]bool{}
		var keys []string
		allScan := s.store.FrontierPaths()
		for _, p := range allScan {
			// Same production rule as KEYS: collapse only under real hashes.
			key := p
			if idx := strings.LastIndex(p, ":"); idx > 0 {
				if s.isHashKey(p[:idx]) {
					key = p[:idx]
				}
			}
			if prefix == "" || strings.HasPrefix(key, prefix) || strings.HasPrefix(p, prefix) {
				if !seenScan[key] {
					seenScan[key] = true
					keys = append(keys, key)
				}
			}
		}
		sort.Strings(keys)
		if len(keys) > count {
			keys = keys[:count]
		}
		resp := "*2\r\n:0\r\n"
		resp += fmt.Sprintf("*%d\r\n", len(keys))
		for _, k := range keys {
			resp += bulkString(k)
		}
		return resp, true

	case "INFO":
		uptime := time.Since(serverStartTime).Round(time.Second)
		keys := len(s.store.FrontierPaths())
		resp := fmt.Sprintf("wavicle_version:1.0\r\nuptime:%s\r\nkeys:%d\r\nshards:32\r\n", uptime, keys)
		return formatBulkString(resp), true

	case "HELLO":
		return "%1\r\n+server\r\n+wavicle\r\n", true

	case "CLIENT", "CONFIG", "SELECT", "AUTH":
		return "+OK\r\n", true

	case "FLUSHDB", "FLUSHALL":
		for _, p := range s.store.FrontierPaths() {
			_, _ = s.store.AppendAtom(&core.EConst{Value: core.VNull{}}, p, nil, time.Time{})
		}
		s.clearKeyTypes()
		return "+OK\r\n", true

	case "PEXPIRE":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		ms, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil || ms <= 0 {
			return "ERR invalid expire time\r\n", true
		}
		cur, ok := s.store.GetCurrent(args[1])
		if !ok || isTombstone(cur) {
			return ":0\r\n", true
		}
		_, _ = s.store.AppendAtom(cur.Expr, args[1], nil, time.Now().Add(time.Duration(ms)*time.Millisecond))
		return ":1\r\n", true

	case "PTTL":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		cur, ok := s.store.GetCurrent(args[1])
		if !ok || isTombstone(cur) {
			return ":-2\r\n", true
		}
		if cur.ExpiresAt.IsZero() {
			return ":-1\r\n", true
		}
		ms := time.Until(cur.ExpiresAt).Milliseconds()
		if ms <= 0 {
			return ":-2\r\n", true
		}
		return fmt.Sprintf(":%d\r\n", ms), true

	case "PERSIST":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		cur, ok := s.store.GetCurrent(args[1])
		if !ok || isTombstone(cur) {
			return ":0\r\n", true
		}
		if cur.ExpiresAt.IsZero() {
			return ":0\r\n", true
		}
		_, _ = s.store.AppendAtom(cur.Expr, args[1], nil, time.Time{})
		return ":1\r\n", true

	case "EVAL", "EVALSHA", "SCRIPT", "FUNCTION":
		return "NOSCRIPT Lua scripting is disabled in Wavicle proof mode\r\n", true

	case "XADD":
		// XADD key ID field val [field val...] — minimal stream over VArray.
		if len(args) < 5 || (len(args)-3)%2 != 0 {
			return "ERR wrong number of arguments\r\n", true
		}
		telemetry.Get().RecordRequest(cmd)
		if err := s.assertKeyType(args[1], "stream"); err != nil {
			r, _ := wrongType()
			return r, true
		}
		id := args[2]
		if id == "*" {
			id = fmt.Sprintf("%d-0", time.Now().UnixMilli())
		} else if _, _, ok := parseStreamID(id); !ok {
			return "ERR invalid stream ID, expected <millis>-<seq>\r\n", true
		}
		rec := core.VRecord{"_id": core.VString(id)}
		for i := 3; i < len(args); i += 2 {
			rec[args[i]] = core.VString(args[i+1])
		}
		var arr core.VArray
		if v, _, ok := curValue(s, args[1]); ok {
			a, isArr := v.(core.VArray)
			if !isArr {
				r, _ := wrongType()
				return r, true
			}
			arr = a
		}
		arr = append(arr, rec)
		_ = putValue(s, args[1], arr)
		s.setKeyType(args[1], "stream")
		return formatBulkString(id), true

	case "XLEN":
		if len(args) < 2 {
			return "ERR wrong number of arguments\r\n", true
		}
		if v, _, ok := curValue(s, args[1]); ok {
			if arr, isArr := v.(core.VArray); isArr {
				return fmt.Sprintf(":%d\r\n", len(arr)), true
			}
			r, _ := wrongType()
			return r, true
		}
		return ":0\r\n", true

	case "XRANGE":
		if len(args) < 4 {
			return "ERR wrong number of arguments\r\n", true
		}
		v, _, ok := curValue(s, args[1])
		if !ok {
			return "*0\r\n", true
		}
		arr, isArr := v.(core.VArray)
		if !isArr {
			r, _ := wrongType()
			return r, true
		}
		// Production: numeric <millis>-<seq> compare. Lexicographic string
		// compare breaks across digit widths ("999-0" > "1000-0"
		// lexicographically). "-" and "+" are open bounds.
		min, max := args[2], args[3]
		var minMS, minSeq int64
		var maxMS, maxSeq int64
		var minOK, maxOK bool
		if min != "-" {
			if ms, sq, ok := parseStreamID(min); ok {
				minMS, minSeq, minOK = ms, sq, true
			} else {
				return "ERR invalid stream ID, expected <millis>-<seq> or -\r\n", true
			}
		}
		if max != "+" {
			if ms, sq, ok := parseStreamID(max); ok {
				maxMS, maxSeq, maxOK = ms, sq, true
			} else {
				return "ERR invalid stream ID, expected <millis>-<seq> or +\r\n", true
			}
		}
		var filtered []core.Value
		for _, e := range arr {
			rec, _ := e.(core.VRecord)
			id := ""
			if iv, ok := rec["_id"]; ok {
				if str, ok := toString(iv); ok {
					id = str
				}
			}
			ms, sq, ok := parseStreamID(id)
			if !ok {
				continue // skip corrupt entries rather than mis-ordering
			}
			if minOK && (ms < minMS || (ms == minMS && sq < minSeq)) {
				continue
			}
			if maxOK && (ms > maxMS || (ms == maxMS && sq > maxSeq)) {
				continue
			}
			filtered = append(filtered, e)
		}
		resp := fmt.Sprintf("*%d\r\n", len(filtered))
		for _, e := range filtered {
			rec, _ := e.(core.VRecord)
			resp += fmt.Sprintf("*2\r\n")
			id := ""
			if iv, ok := rec["_id"]; ok {
				if str, ok := toString(iv); ok {
					id = str
				}
			}
			resp += bulkString(id)
			keys := make([]string, 0, len(rec))
			for k := range rec {
				if k != "_id" {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			resp += fmt.Sprintf("*%d\r\n", len(keys)*2)
			for _, k := range keys {
				resp += bulkString(k)
				if str, ok := toString(rec[k]); ok {
					resp += bulkString(str)
				} else {
					resp += bulkString(fmt.Sprint(rec[k]))
				}
			}
		}
		return resp, true
	}
	return "", false
}

// parseStreamID parses "<millis>-<seq>" numerically.
func parseStreamID(id string) (ms, seq int64, ok bool) {
	parts := strings.SplitN(id, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	ms, err1 := strconv.ParseInt(parts[0], 10, 64)
	sq, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || ms < 0 || sq < 0 {
		return 0, 0, false
	}
	return ms, sq, true
}

func (s *Server) handleHashExtended(args []string) (string, bool) {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "HMSET":
		if len(args) < 4 || (len(args)-2)%2 != 0 {
			return "ERR wrong number of arguments\r\n", true
		}
		key := args[1]
		if err := s.assertKeyType(key, "hash"); err != nil {
			// Absent key ("none") passes; wrong concrete type refuses.
			if s.keyTypeOf(key) != "none" {
				r, _ := wrongType()
				return r, true
			}
		}
		for i := 2; i < len(args); i += 2 {
			hp := key + ":" + args[i]
			_ = putValue(s, hp, core.VString(args[i+1]))
		}
		s.setKeyType(key, "hash")
		return "+OK\r\n", true
	case "HMGET":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		key := args[1]
		resp := fmt.Sprintf("*%d\r\n", len(args)-2)
		for _, f := range args[2:] {
			hp := key + ":" + f
			if v, _, ok := curValue(s, hp); ok {
				if str, isStr := toString(v); isStr {
					resp += formatBulkString(str)
					continue
				}
			}
			resp += "$-1\r\n"
		}
		return resp, true
	case "HDEL":
		if len(args) < 3 {
			return "ERR wrong number of arguments\r\n", true
		}
		deleted := 0
		for _, f := range args[2:] {
			hp := args[1] + ":" + f
			if _, ok := s.store.GetCurrent(hp); ok {
				_, _ = s.store.AppendAtom(&core.EConst{Value: core.VNull{}}, hp, nil, time.Time{})
				deleted++
			}
		}
		// Drop hash type when last field is gone (Redis deletes empty hash).
		if s.keyTypeOf(args[1]) == "hash" {
			prefix := args[1] + ":"
			remain := false
			for _, p := range s.store.FrontierPaths() {
				if strings.HasPrefix(p, prefix) {
					if _, _, ok := curValue(s, p); ok {
						remain = true
						break
					}
				}
			}
			if !remain {
				s.delKeyType(args[1])
			}
		}
		return fmt.Sprintf(":%d\r\n", deleted), true
	case "HLEN", "HKEYS", "HVALS", "HEXISTS":
		if len(args) < 2+(map[string]int{"HEXISTS": 1}[cmd]) {
			return "ERR wrong number of arguments\r\n", true
		}
		prefix := args[1] + ":"
		fields := map[string]string{}
		for _, p := range s.store.FrontierPaths() {
			if strings.HasPrefix(p, prefix) {
				if v, _, ok := curValue(s, p); ok {
					if str, isStr := toString(v); isStr {
						fields[strings.TrimPrefix(p, prefix)] = str
					}
				}
			}
		}
		switch cmd {
		case "HLEN":
			return fmt.Sprintf(":%d\r\n", len(fields)), true
		case "HEXISTS":
			if _, ok := fields[args[2]]; ok {
				return ":1\r\n", true
			}
			return ":0\r\n", true
		case "HKEYS":
			keys := make([]string, 0, len(fields))
			for k := range fields {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			resp := fmt.Sprintf("*%d\r\n", len(keys))
			for _, k := range keys {
				resp += bulkString(k)
			}
			return resp, true
		case "HVALS":
			keys := make([]string, 0, len(fields))
			for k := range fields {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			resp := fmt.Sprintf("*%d\r\n", len(keys))
			for _, k := range keys {
				resp += formatBulkString(fields[k])
			}
			return resp, true
		}
	}
	return "", false
}
