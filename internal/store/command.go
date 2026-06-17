package store

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)


var cmdTable = map[string]*command{}

type Exec func(db kVStore, args [][]byte) resp.RespType

type command struct {
	arity int
	exec Exec
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

func execGet(db kVStore, args [][]byte) resp.RespType {
	key := string(args[0])
	data, ok := db.get(key)
	if !ok {
		return &resp.Nil{}
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

	return &resp.Nil{}
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


func registerCommand(name string, arity int, exec Exec) *command {
	c := &command{
		arity: arity,
		exec: exec,
	}

	cmdTable[name] = c	
	return c
}

func init() {
	registerCommand("set", -3, execSet)
	registerCommand("get", 2, execGet)
	registerCommand("ttl", 2, execTtl)
}
