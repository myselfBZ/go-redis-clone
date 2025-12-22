package main

import (
	"fmt"
	"log/slog"
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
		return resp.WriteError(conn, "invalid number of args to GET. Expected 1 got "+ fmt.Sprintf("%d", len(args)))
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

			setArgs.EX = seconds
			i++
		case "PX":
			if i + 1 >= len(args) {
				return resp.WriteError(conn, "invalid syntax")
			}

			seconds, err := strconv.Atoi(args[i + 1].(*resp.BulkStr).Data)

			if err != nil {
				return resp.WriteError(conn, "invalid syntax")
			}

			setArgs.PX = seconds
			i++
		default:
			return resp.WriteError(conn, "invalid options")
		}
	}

	if err := s.storage.Set(setArgs); err != nil {
		return resp.WriteError(conn, err.Error())
	}

	return resp.WriteOK(conn)
}


func (s *server) handleCommandDocs(conn net.Conn, args []resp.RespType) error {
	return resp.WriteOK(conn)
}

func (s *server) handleDel(conn net.Conn, args []resp.RespType) error {
	slog.Info("i am here")
	found := 0
	for _, arg := range args[1:] {
		keyBulkStr, ok := arg.(*resp.BulkStr)
		if !ok {
			continue
		}
		slog.Info("Delete operation..")
		err := s.storage.Del(keyBulkStr.Data)
		if err == nil {
			found += 1
		}
		slog.Info("hell is there ")
	}
	slog.Info("loop is over")
	return resp.WriteBulkStr(conn, fmt.Sprintf("(integer) %d", found))
}


