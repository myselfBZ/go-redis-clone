package main

import (
	"net"
	"strconv"
	"strings"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
	"github.com/myselfBZ/go-redis-clone/internal/store"
)

var commandHandlers = map[resp.CommandType]Handler{}

type Handler func(net.Conn, []resp.RespType) error


func (s *server) handleGet(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2 {
		return resp.WriteError(conn, "wrong number of args to 'get' ")
	}

	key, ok := args[1].(*resp.BulkStr)

	if !ok{
		return resp.WriteError(conn, "keys must be bulk strings")
	}

	val, err := s.storage.Get(key.Data)
	if err != nil {
		return resp.WriteNil(conn)
	}
	return resp.WriteRespType(conn, val)
}

func (s *server) handleSet(conn net.Conn, args []resp.RespType) error {
	if len(args) < 3 {
		return resp.WriteError(conn, "invalid syntax")
	}
	key, value := args[1], args[2]

	setArgs := store.SetArgs{
		Key: key.(*resp.BulkStr).Data,
		Value: value,
	}

	for i := 3; i < len(args); i++ {
		bulkStr := args[i].(*resp.BulkStr)

		switch strings.ToUpper(bulkStr.Data) {
		case "XX":
			setArgs.XX = true
		case "NX":
			setArgs.NX = true
		case "EX":

			if i + 1 >= len(args) {
				return resp.WriteError(conn, "invalid syntax")
			}

			seconds, err := strconv.Atoi(args[i + 1].(*resp.BulkStr).Data)

			if err != nil {
				return resp.WriteError(conn, "invalid syntax")
			}

			if seconds <= 0 {
				seconds = -1
			}

			setArgs.EX = seconds
			i++
		case "PX":
			if i + 1 >= len(args) {
				return resp.WriteError(conn, "invalid syntax")
			}

			milliseconds, err := strconv.Atoi(args[i + 1].(*resp.BulkStr).Data)

			if err != nil {
				return resp.WriteError(conn, "invalid syntax")
			}

			if milliseconds <= 0 {
				milliseconds = -1
			}

			setArgs.PX = milliseconds 
			i++
		default:
			return resp.WriteError(conn, "invalid options")
		}
	}

	if setArgs.EX > 0 && setArgs.PX > 0 {
		return resp.WriteError(conn, "EX and PX options at the same time are not compatible")
	}

	if setArgs.EX < 0 || setArgs.PX < 0 {
		return resp.WriteError(conn, "invalid expire time in 'set' command")
	}

	if setArgs.XX && setArgs.NX {
		return resp.WriteError(conn, "XX and NX options at the same time are not compatible")
	}

	if written := s.storage.Set(setArgs); !written {
		return resp.WriteNil(conn)
	}

	return resp.WriteOK(conn)
}


func (s *server) handleTTL(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2  {
		return resp.WriteError(conn, "invalid syntax")
	}

	result := s.storage.TTL(args[1].(*resp.BulkStr).Data)
	return resp.WriteInt(conn, result)
}

func (s *server) handlePTTL(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2  {
		return resp.WriteError(conn, "invalid syntax")
	}

	result := s.storage.PTTL(args[1].(*resp.BulkStr).Data)
	return resp.WriteInt(conn, result)
}

func (s *server) handleCommandDocs(conn net.Conn, args []resp.RespType) error {
	return resp.WriteOK(conn)
}

func (s *server) handleDel(conn net.Conn, args []resp.RespType) error {
	found := 0
	for _, arg := range args[1:] {
		keyBulkStr, ok := arg.(*resp.BulkStr)
		if !ok {
			continue
		}
		err := s.storage.Del(keyBulkStr.Data)
		if err == nil {
			found += 1
		}
	}
	return resp.WriteInt(conn, found)
}

func (s *server) handleExpire(conn net.Conn, args []resp.RespType) error {
	if len(args) < 3 {
		return resp.WriteError(conn, "invalid syntax")
	}

	seconds, err := strconv.Atoi(args[2].(*resp.BulkStr).Data)

	if err != nil {
		return resp.WriteError(conn, "value is not an integer or out of range")
	}
	expireArgs  := store.ExpireArgs{
		Key: args[1].(*resp.BulkStr).Data,
		Seconds: seconds,
	}

	for i := 3; i < len(args); i++ {
		bulkStr := args[i].(*resp.BulkStr)

		switch strings.ToUpper(bulkStr.Data) {
		case "XX":
			expireArgs.XX = true
		case "NX":
			expireArgs.NX = true
		default:
			return resp.WriteError(conn, "invalid options")
		}
	}

	if expireArgs.XX && expireArgs.NX {
		return resp.WriteError(conn, "EX and PX can't have non-zero value at a time")
	}

	if written := s.storage.Expire(expireArgs); !written {
		return resp.WriteInt(conn, 0)
	}

	return resp.WriteInt(conn, 1)
}

func (s *server) handlePersist(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2 {
		return resp.WriteError(conn, "wrong number of arguments for 'persist' command")
	}

	written := s.storage.Persist(args[1].(*resp.BulkStr).Data)

	if !written {
		return resp.WriteInt(conn, 0)
	}

	return resp.WriteInt(conn, 1)
}
