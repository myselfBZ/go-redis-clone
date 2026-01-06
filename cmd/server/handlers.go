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

type Handler func(net.Conn, []resp.RespType) resp.Response

var commandHandlers = map[resp.CommandType]Handler{}

func (s *server) handlePing(conn net.Conn, args []resp.RespType) resp.Response {
	return resp.Response{
		Data: &resp.SimpleStr{
			Data: []byte("PONG"),
		},
		Success: true,
	}
}

func (s *server) handleGet(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("wrong number of arguments to 'get'"),
			},
		}
	}

	key, ok := args[1].(*resp.BulkStr)

	if !ok {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("key must be a bulk string"),
			},
		}
	}

	val, err := s.storage.Get(key.String())

	if err != nil {
		return resp.Response{
			Data: &resp.Nil{},
		}
	}

	respVal, err := fromStoreToResp(val)

	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("server encoutered a problem"),
			},
		}
	}

	return resp.Response{
		Data:    respVal,
		Success: true,
	}
}

func (s *server) handleSet(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) < 3 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("invalid syntax"),
			},
		}
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

			if i+1 >= len(args) {
				return resp.Response{
					Data: &resp.RespErr{
						Data: []byte("invalid syntax"),
					},
				}
			}

			seconds, err := strconv.Atoi(args[i+1].(*resp.BulkStr).String())

			if err != nil {
				return resp.Response{
					Data: &resp.RespErr{
						Data: []byte("invalid syntax"),
					},
				}
			}

			if seconds <= 0 {
				seconds = -1
			}

			setArgs.EX = seconds
			i++
		case "PX":
			if i+1 >= len(args) {
				return resp.Response{
					Data: &resp.RespErr{
						Data: []byte("invalid syntax"),
					},
				}
			}

			milliseconds, err := strconv.Atoi(args[i+1].(*resp.BulkStr).String())

			if err != nil {
				return resp.Response{
					Data: &resp.RespErr{
						Data: []byte("invalid syntax"),
					},
				}
			}

			if milliseconds <= 0 {
				milliseconds = -1
			}

			setArgs.PX = milliseconds
			i++
		default:
			return resp.Response{
				Data: &resp.RespErr{
					Data: []byte("invalid syntax"),
				},
			}
		}
	}

	if setArgs.EX > 0 && setArgs.PX > 0 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("invalid syntax"),
			},
		}
	}

	if setArgs.EX < 0 || setArgs.PX < 0 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("EX and PX options at the same time are not compatible"),
			},
		}
	}

	if setArgs.XX && setArgs.NX {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("XX and NX options at the same time are not compatible"),
			},
		}
	}

	if written := s.storage.Set(setArgs); !written {
		return resp.Response{
			Data: &resp.Nil{},
		}
	}

	return resp.Response{
		Data: &resp.SimpleStr{
			Data: []byte("OK"),
		},
		Success: true,
	}
}

func (s *server) handleTTL(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("invalid syntax"),
			},
		}
	}

	result := s.storage.TTL(args[1].(*resp.BulkStr).String())

	return resp.Response{
		Data: &resp.Intiger{
			Data: result,
		},
		Success: result != -1 && result != -2,
	}
}

func (s *server) handlePTTL(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("invalid syntax"),
			},
		}

	}

	result := s.storage.PTTL(args[1].(*resp.BulkStr).String())
	return resp.Response{
		Data: &resp.Intiger{
			Data: result,
		},
		Success: result != -1 && result != -2,
	}
}

func (s *server) handleCommandDocs(conn net.Conn, args []resp.RespType) resp.Response {
	return resp.Response{
		Data: &resp.SimpleStr{
			Data: []byte("OK"),
		},
		Success: true,
	}
}

func (s *server) handleDel(conn net.Conn, args []resp.RespType) resp.Response {
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
	return resp.Response{
		Data: &resp.Intiger{
			Data: found,
		},
		Success: found > 0,
	}
}

func (s *server) handleExpire(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) < 3 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("invalid syntax"),
			},
		}
	}

	seconds, err := strconv.Atoi(args[2].(*resp.BulkStr).String())

	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("value is not an intiger or out of range"),
			},
		}
	}
	expireArgs := store.ExpireArgs{
		Key:     args[1].(*resp.BulkStr).String(),
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
			return resp.Response{
				Data: &resp.RespErr{
					Data: []byte("invalid syntax"),
				},
			}
		}
	}

	if expireArgs.XX && expireArgs.NX {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("XX and NX options at the same time are not compatible"),
			},
		}
	}

	if written := s.storage.Expire(expireArgs); !written {
		return resp.Response{
			Data: &resp.Intiger{
				Data: 0,
			},
		}
	}

	return resp.Response{
		Data: &resp.Intiger{
			Data: 1,
		},
		Success: true,
	}
}

func (s *server) handlePersist(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("wrong number of arguments to 'persist'"),
			},
		}
	}

	written := s.storage.Persist(args[1].(*resp.BulkStr).String())

	if !written {
		return resp.Response{
			Data: &resp.Intiger{
				Data: 0,
			},
			Success: true,
		}
	}

	return resp.Response{
		Data: &resp.Intiger{
			Data: 1,
		},
		Success: true,
	}
}

func (s *server) handleDecr(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("wrong number of arguments to 'decr'"),
			},
		}
	}

	key := args[1].(*resp.BulkStr)

	val, err := s.storage.Decr(key.String())
	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("value is not an intiger or out of range"),
			},
		}
	}
	return resp.Response{
		Data: &resp.Intiger{
			Data: val,
		},
		Success: true,
	}
}

func (s *server) handleIncrBy(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) != 3 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("wrong number of arguments to 'incrby'"),
			},
		}
	}

	key := args[1].(*resp.BulkStr)
	incrBy := args[2].(*resp.BulkStr)
	integer, err := strconv.Atoi(incrBy.String())

	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("value is not an intiger or out of range"),
			},
		}
	}

	val, err := s.storage.IncrBy(key.String(), integer)
	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("value is not an intiger or out of range"),
			},
		}
	}
	return resp.Response{
		Data: &resp.Intiger{
			Data: val,
		},
		Success: true,
	}
}

func (s *server) handleIncr(conn net.Conn, args []resp.RespType) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("wrong number of arguments to 'incr'"),
			},
		}
	}

	key := args[1].(*resp.BulkStr)

	val, err := s.storage.Incr(key.String())
	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("value is not an intiger or out of range"),
			},
		}
	}
	return resp.Response{
		Data: &resp.Intiger{
			Data: val,
		},
		Success: true,
	}
}

func fromStoreToResp(v store.Value) (resp.RespType, error) {
	t := v.StorageValueType()
	switch t {
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
