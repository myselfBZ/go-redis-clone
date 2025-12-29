package main

import (
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
	"github.com/myselfBZ/go-redis-clone/internal/store"
)

var ErrUnknownStoreType = errors.New("unknown storage level type")

var commandHandlers = map[resp.CommandType]Handler{}

type Handler func(net.Conn, []resp.RespType) error


func (s *server) handlePing(conn net.Conn, args []resp.RespType) error {
	return resp.WritePong(conn)
}

func (s *server) handleGet(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2 {
		return resp.WriteError(conn, "wrong number of args to 'get' ")
	}

	key, ok := args[1].(*resp.BulkStr)

	if !ok{
		return resp.WriteError(conn, "keys must be bulk strings")
	}

	val, err := s.storage.Get(key.String())
	if err != nil {
		return resp.WriteNil(conn)
	}
	respVal, err := fromStoreToResp(val)

	if err != nil {
		return resp.WriteError(conn, "server encountered a problem")
	}

	return resp.WriteRespType(conn, respVal)
}

func (s *server) handleSet(conn net.Conn, args []resp.RespType) error {
	if len(args) < 3 {
		return resp.WriteError(conn, "invalid syntax")
	}
	key, value := args[1], args[2]

	setArgs := store.SetArgs{
		Key: key.(*resp.BulkStr).String(),
	}

	// might be an integer
	parsedValue := value.(*resp.BulkStr)
	intVal, err := strconv.Atoi(parsedValue.String())
	
	if err != nil {
		setArgs.Value = &store.StringValue{
			Data: parsedValue.Data,
		}
	} else {
		setArgs.Value = &store.IntValue{
			Data: intVal,
		}
	}


	for i := 3; i < len(args); i++ {
		bulkStr := args[i].(*resp.BulkStr)

		switch strings.ToUpper(bulkStr.String()) {
		case "XX":
			setArgs.XX = true
		case "NX":
			setArgs.NX = true
		case "EX":

			if i + 1 >= len(args) {
				return resp.WriteError(conn, "invalid syntax")
			}

			seconds, err := strconv.Atoi(args[i + 1].(*resp.BulkStr).String())

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

			milliseconds, err := strconv.Atoi(args[i + 1].(*resp.BulkStr).String())

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

	keysStored.Inc()
	return resp.WriteOK(conn)
}


func (s *server) handleTTL(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2  {
		return resp.WriteError(conn, "invalid syntax")
	}

	result := s.storage.TTL(args[1].(*resp.BulkStr).String())
	return resp.WriteInt(conn, result)
}

func (s *server) handlePTTL(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2  {
		return resp.WriteError(conn, "invalid syntax")
	}

	result := s.storage.PTTL(args[1].(*resp.BulkStr).String())
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
		err := s.storage.Del(keyBulkStr.String())
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

	seconds, err := strconv.Atoi(args[2].(*resp.BulkStr).String())

	if err != nil {
		return resp.WriteError(conn, "value is not an integer or out of range")
	}
	expireArgs  := store.ExpireArgs{
		Key: args[1].(*resp.BulkStr).String(),
		Seconds: seconds,
	}

	for i := 3; i < len(args); i++ {
		bulkStr := args[i].(*resp.BulkStr)

		switch strings.ToUpper(bulkStr.String()) {
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

	written := s.storage.Persist(args[1].(*resp.BulkStr).String())

	if !written {
		return resp.WriteInt(conn, 0)
	}

	return resp.WriteInt(conn, 1)
}

func (s *server) handleDecr(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2 {
		return resp.WriteError(conn, "wrong number of arguments for 'decr' command")
	}

	key := args[1].(*resp.BulkStr)

	val, err := s.storage.Decr(key.String())
	if err != nil {
		return resp.WriteError(conn, "value is not an integer or out of range")
	}
	return resp.WriteInt(conn, val)
}

func (s *server) handleIncrBy(conn net.Conn, args []resp.RespType) error {
	if len(args) != 3 {
		return resp.WriteError(conn, "wrong number of arguments for 'incrby' command")
	}

	key := args[1].(*resp.BulkStr)
	incrBy := args[2].(*resp.BulkStr)
	integer, err := strconv.Atoi(incrBy.String())

	if err != nil {
		return resp.WriteError(conn, "value is not an integer or out of range")
	}

	val, err := s.storage.IncrBy(key.String(), integer)
	if err != nil {
		return resp.WriteError(conn, "value is not an integer or out of range")
	}
	return resp.WriteInt(conn, val)
}

func (s *server) handleIncr(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2 {
		return resp.WriteError(conn, "wrong number of arguments for 'incr' command")
	}

	key := args[1].(*resp.BulkStr)

	val, err := s.storage.Incr(key.String())
	if err != nil {
		return resp.WriteError(conn, "value is not an integer or out of range")
	}
	return resp.WriteInt(conn, val)
}


func fromStoreToResp(v store.Value) (resp.RespType, error) {
	t := v.StorageValueType()
	switch  t {
	case store.Int:
		return &resp.Intiger{
			Data: v.(*store.IntValue).Data,
		}, nil
	case store.String:
		return &resp.BulkStr{
			Data: v.(*store.StringValue).Data,
		}, nil
	default:
		return nil, ErrUnknownStoreType
	}
}





