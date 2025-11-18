package main

import (
	"fmt"
	"net"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)

var commandHandlers = map[resp.CommandType]Handler{}

type Handler func(net.Conn, []resp.RespType) error


func (s *server) handleGet(conn net.Conn, args []resp.RespType) error {
	if len(args) != 2 {
		return resp.WriteError(conn, "invalid number of args to GET. Expected 2 got "+ fmt.Sprintf("%d", len(args)))
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
	if (len(args) - 1) % 2 != 0 {
		return resp.WriteError(conn, "invalid number of args to SET")
	}

	for i := 1; i < len(args) - 1; i+=2 {
		key, val := args[i], args[i + 1]
		keyBulkStr, ok  := key.(*resp.BulkStr)

		if !ok {
			return resp.WriteError(conn, "keys must be bulk strings")
		}

		s.storage.Set(keyBulkStr.Data, val)
	}

	return resp.WriteOK(conn)

}


func (s *server) handleCommandDocs(conn net.Conn, args []resp.RespType) error {
	return resp.WriteOK(conn)
}


