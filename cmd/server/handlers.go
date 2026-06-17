package main

import (
	"strconv"
	"strings"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
	"github.com/myselfBZ/go-redis-clone/internal/store"
)


type Handler func([][]byte) resp.Response

var commandHandlers = map[resp.CommandType]Handler{}

func (s *server) handlePing(args [][]byte) resp.Response {
	return resp.Response{
		Data: &resp.SimpleStr{
			Data: []byte("PONG"),
		},
		Success: true,
	}
}

func (s *server) handleGet(args [][]byte) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR wrong number of arguments to 'get'"),
			},
		}
	}

	key := string(args[1])

	val, err := s.storage.Get(key)

	if err != nil {
		return resp.Response{
			Data: &resp.BulkStr{
				Data: nil,
			},
		}
	}

	respVal := &resp.BulkStr{
		Data: val,
	}

	return resp.Response{
		Data:    respVal,
		Success: true,
	}
}

func (s *server) handleSet(args [][]byte) resp.Response {
	if len(args) < 3 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR invalid syntax"),
			},
		}
	}
	key, value := args[1], args[2]

	setArgs := store.SetArgs{
		Key: string(key),
	}

	setArgs.Value = value

	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "XX":
			setArgs.XX = true
		case "NX":
			setArgs.NX = true
		case "EX":

			if i+1 >= len(args) {
				return resp.Response{
					Data: &resp.RespErr{
						Data: []byte("ERR invalid syntax"),
					},
				}
			}

			seconds, err := strconv.Atoi(string(args[i+1]))

			if err != nil {
				return resp.Response{
					Data: &resp.RespErr{
						Data: []byte("ERR invalid syntax"),
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
						Data: []byte("ERR invalid syntax"),
					},
				}
			}

			milliseconds, err := strconv.Atoi(string(args[i+1]))

			if err != nil {
				return resp.Response{
					Data: &resp.RespErr{
						Data: []byte("ERR invalid syntax"),
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
					Data: []byte("ERR invalid syntax"),
				},
			}
		}
	}

	if setArgs.EX > 0 && setArgs.PX > 0 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR EX and PX options at the same time are not compatible"),
			},
		}
	}

	if setArgs.EX < 0 || setArgs.PX < 0 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR EX and PX options at the same time are not compatible"),
			},
		}
	}

	if setArgs.XX && setArgs.NX {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR XX and NX options at the same time are not compatible"),
			},
		}
	}

	if written := s.storage.Set(setArgs); !written {
		return resp.Response{
			Data: &resp.BulkStr{
				Data: nil,
			},
		}
	}

	return resp.Response{
		Data: &resp.SimpleStr{
			Data: []byte("OK"),
		},
		Success: true,
	}
}

func (s *server) handleTTL( args [][]byte) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR invalid syntax"),
			},
		}
	}

	result := s.storage.TTL(string(args[1]))

	return resp.Response{
		Data: &resp.Intiger{
			Data: result,
		},
		Success: result != -1 && result != -2,
	}
}

func (s *server) handlePTTL( args [][]byte) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR invalid syntax"),
			},
		}

	}

	result := s.storage.PTTL(string(args[1]))
	return resp.Response{
		Data: &resp.Intiger{
			Data: result,
		},
		Success: result != -1 && result != -2,
	}
}

func (s *server) handleCommandDocs( args [][]byte) resp.Response {
	return resp.Response{
		Data: &resp.SimpleStr{
			Data: []byte("OK"),
		},
		Success: true,
	}
}

func (s *server) handleDel(args [][]byte) resp.Response {
	var found int64 = 0

	for _, arg := range args[1:] {
		err := s.storage.Del(string(arg))
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

func (s *server) handleExpire(args [][]byte) resp.Response {
	if len(args) < 3 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR invalid syntax"),
			},
		}
	}

	seconds, err := strconv.Atoi(string(args[2]))

	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR value is not an intiger or out of range"),
			},
		}
	}
	expireArgs := store.ExpireArgs{
		Key:     string(args[1]),
		Seconds: seconds,
	}

	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "XX":
			expireArgs.XX = true
		case "NX":
			expireArgs.NX = true
		default:
			return resp.Response{
				Data: &resp.RespErr{
					Data: []byte("ERR invalid syntax"),
				},
			}
		}
	}

	if expireArgs.XX && expireArgs.NX {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR XX and NX options at the same time are not compatible"),
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

func (s *server) handlePersist( args [][]byte) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR wrong number of arguments to 'persist'"),
			},
		}
	}

	written := s.storage.Persist(string(args[1]))

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

func (s *server) handleDecr(args [][]byte) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR wrong number of arguments to 'decr'"),
			},
		}
	}

	key := args[1]

	val, err := s.storage.Decr(string(key))
	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR value is not an intiger or out of range"),
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

func (s *server) handleIncrBy(args [][]byte) resp.Response {
	if len(args) != 3 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR wrong number of arguments to 'incrby'"),
			},
		}
	}

	key := args[1]
	incrBy := args[2]
	integer, err := strconv.ParseInt(string(incrBy), 10, 64)

	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR value is not an intiger or out of range"),
			},
		}
	}

	val, err := s.storage.IncrBy(string(key), integer)
	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR value is not an intiger or out of range"),
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

func (s *server) handleIncr(args [][]byte) resp.Response {
	if len(args) != 2 {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR wrong number of arguments to 'incr'"),
			},
		}
	}

	key := args[1]

	val, err := s.storage.Incr(string(key))
	if err != nil {
		return resp.Response{
			Data: &resp.RespErr{
				Data: []byte("ERR value is not an intiger or out of range"),
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
