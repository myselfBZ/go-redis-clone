package store

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)

var cmdTable = map[string]*command{}

type execCmd func(db kVStore, args [][]byte) resp.RespType

type command struct {
	arity int
	exec  execCmd
}

func execExpire(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])
	seconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.NotInErr()
	}

	incompatibleErr := resp.MakeErr("ERR NX and XX, GT or LT options at the same time are not compatible")

	opt := ""

	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			if opt != "" {
				return incompatibleErr
			}
			opt = "NX"

		case "XX":
			if opt != "" {
				return incompatibleErr
			}
			opt = "XX"

		case "GT":
			if opt != "" {
				return incompatibleErr
			}

			opt = "GT"
		case "LT":
			if opt != "" {
				return incompatibleErr
			}

			opt = "LT"
		default:
			return resp.SyntaxErr()
		}
	}
	result := &resp.Intiger{Data: 0}

	_, ok := db.get(key)

	if ok {
		newExpiry := time.Now().Add(time.Duration(seconds) * time.Second)
		at, expires := db.getExpiresAt(key)

		if opt == "" {
			db.expire(key, newExpiry)
			result.Data = 1
		}

		if opt == "NX" && !expires {
			db.expire(key, newExpiry)
			result.Data = 1
		}

		if opt == "XX" && expires {
			db.expire(key, newExpiry)
			result.Data = 1
		}

		if opt == "GT" && (newExpiry.Sub(at).Milliseconds() > 0) {
			db.expire(key, newExpiry)
			result.Data = 1
		}

		if opt == "LT" && (newExpiry.Sub(at).Milliseconds() < 0) {
			db.expire(key, newExpiry)
			result.Data = 1
		}

		return result
	}

	return &resp.Intiger{
		Data: 0,
	}
}

func execDecrBy(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])

	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	delta = -delta
	if err != nil {
		return resp.NotInErr()
	}

	d, ok := db.get(key)
	if !ok {
		db.put(key, &dataEntity{
			val: args[1],
		})
		return &resp.Intiger{Data: delta}
	}

	// TODO: caution with type cast, as new types (lists, hashes) might be introduced
	intVal, err := strconv.ParseInt(string(d.val.([]byte)), 10, 64)
	if err != nil {
		return resp.NotInErr()
	}

	intVal += delta

	d.val = []byte(strconv.FormatInt(intVal, 10))
	return &resp.Intiger{
		Data: intVal,
	}
}

func execIncrBy(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])

	rawDelta := string(args[1])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return resp.NotInErr()
	}

	d, ok := db.get(key)
	if !ok {
		db.put(key, &dataEntity{
			val: args[1],
		})
		return &resp.Intiger{Data: delta}
	}

	// TODO: caution with type cast, as new types (lists, hashes) might be introduced
	intVal, err := strconv.ParseInt(string(d.val.([]byte)), 10, 64)
	if err != nil {
		return resp.NotInErr()
	}

	intVal += delta

	d.val = []byte(strconv.FormatInt(intVal, 10))
	return &resp.Intiger{
		Data: intVal,
	}
}

func execPtl(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])
	_, ok := db.get(key)
	if !ok {
		return &resp.Intiger{
			Data: -2,
		}
	}
	t, ok := db.getExpiresAt(key)
	if !ok {
		return &resp.Intiger{
			Data: -1,
		}
	}

	ttl := time.Until(t).Milliseconds()
	return &resp.Intiger{
		Data: ttl,
	}
}

func execDel(db kVStore, args [][]byte) resp.RespType {
	result := 0

	for _, k := range args {
		r := db.remove(string(k))
		result += r
	}

	return &resp.Intiger{
		Data: int64(result),
	}
}

func execPersist(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])
	result := db.persist(key)
	return &resp.Intiger{
		Data: int64(result),
	}
}

func execTtl(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])
	_, ok := db.get(key)
	if !ok {
		return &resp.Intiger{
			Data: -2,
		}
	}
	t, ok := db.getExpiresAt(key)
	if !ok {
		return &resp.Intiger{
			Data: -1,
		}
	}

	ttl := time.Until(t).Seconds()
	return &resp.Intiger{
		Data: int64(math.Round(ttl)),
	}
}

func execDecr(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])
	d, ok := db.get(key)
	if !ok {
		db.put(key, &dataEntity{
			val: []byte("-1"),
		})
		return &resp.Intiger{Data: -1}
	}

	// TODO: caution with type cast, as new types (lists, hashes) might be introduced
	intVal, err := strconv.ParseInt(string(d.val.([]byte)), 10, 64)
	if err != nil {
		return resp.NotInErr()
	}

	intVal--

	d.val = []byte(strconv.FormatInt(intVal, 10))
	return &resp.Intiger{
		Data: intVal,
	}
}

func execIncr(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])
	d, ok := db.get(key)
	if !ok {
		db.put(key, &dataEntity{
			val: []byte("1"),
		})
		return &resp.Intiger{Data: 1}
	}

	// TODO: caution with type cast, as new types (lists, hashes) might be introduced
	intVal, err := strconv.ParseInt(string(d.val.([]byte)), 10, 64)
	if err != nil {
		return resp.NotInErr()
	}

	intVal++

	d.val = []byte(strconv.FormatInt(intVal, 10))
	return &resp.Intiger{
		Data: intVal,
	}
}

func execPing(db kVStore, args[][]byte) resp.RespType {
	if len(args) == 1 {
		arg := args[0]
		return &resp.BulkStr{
			Data: arg,
		}
	}

	if len(args) > 1 {
		return resp.ArgNumErr("ping")
	}

	return &resp.SimpleStr{
		Data: []byte("PONG"),
	}
}

func execGet(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])
	data, ok := db.get(key)
	if !ok {
		return &resp.BulkStr{Data: nil}
	}
	payload := data.val.([]byte)
	return &resp.BulkStr{
		Data: payload,
	}
}

func execSet(db kVStore, args [][]byte) resp.RespType {

	key, val := string(args[0]), args[1]

	insertOpt := ""
	// ms
	var ttl int64 = 0

	if len(args) > 2 {
		for i := 2; i < len(args); i++ {

			switch strings.ToUpper(string(args[i])) {
			case "XX":
				if insertOpt == "NX" {
					return resp.IncompOptionsErr("PX", "XX")
				}
				insertOpt = "XX"
			case "NX":
				if insertOpt == "XX" {
					return resp.IncompOptionsErr("XX", "PX")
				}
				insertOpt = "NX"
			case "PX":
				if ttl > 0 {
					return resp.SyntaxErr()
				}

				if i+1 >= len(args) {
					return resp.SyntaxErr()
				}

				ttlVal, err := strconv.ParseInt(string(args[i+1]), 10, 64)

				if err != nil {
					return resp.SyntaxErr()
				}

				if ttlVal <= 0 {
					return resp.MakeErr("ERR invalid expire time in set")
				}
				ttl = ttlVal
				i++
			case "EX":
				if ttl > 0 {
					return resp.SyntaxErr()
				}

				if i+1 >= len(args) {
					return resp.SyntaxErr()
				}

				ttlVal, err := strconv.ParseInt(string(args[i+1]), 10, 64)

				if err != nil {
					return resp.SyntaxErr()
				}

				if ttlVal <= 0 {
					return resp.MakeErr("ERR invalid expire time in set")
				}
				ttl = ttlVal * 1000
				i++
			default:
				return resp.SyntaxErr()
			}
		}
	}
	result := 0
	entity := &dataEntity{
		val: val,
	}

	if insertOpt == "" {
		result = db.put(key, entity)
	} else if insertOpt == "XX" {
		result = db.putIfExists(key, entity)
	} else if insertOpt == "NX" {
		result = db.putIfAbsent(key, entity)
	}

	if result > 0 {
		if ttl > 0 {
			expiresAt := time.Now().Add(time.Millisecond * time.Duration(ttl))
			db.expire(key, expiresAt)
		}

		return resp.OkReply()
	}

	return &resp.BulkStr{
		Data: nil,
	}
}

func validArity(arity int, actual int) bool {
	if arity > 0 {
		return arity == actual
	}
	return actual >= abs(arity)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func registerCommand(name string, arity int, exec execCmd) *command {
	c := &command{
		arity: arity,
		exec:  exec,
	}

	cmdTable[name] = c
	return c
}

func init() {
	registerCommand("set", -3, execSet)
	registerCommand("get", 2, execGet)
	registerCommand("ttl", 2, execTtl)
	registerCommand("pttl", 2, execPtl)
	registerCommand("del", -2, execDel)
	registerCommand("persist", 2, execPersist)
	registerCommand("incr", 2, execIncr)
	registerCommand("decr", 2, execDecr)
	registerCommand("incrby", 3, execIncrBy)
	registerCommand("decrby", 3, execDecrBy)
	registerCommand("expire", -3, execExpire)
	registerCommand("ping", -1, execPing)
}
