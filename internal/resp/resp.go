package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	ErrInvalidLength = errors.New("invalid length")
	ErrInvalidValue  = errors.New("invalid value")
	ErrInvalidType   = errors.New("invalid type")
)

var (
	CRLF = []byte("\r\n")
)

const (
	EXPIRE  CommandType = "EXPIRE"
	TTL     CommandType = "TTL"
	PTTL    CommandType = "PTTL"
	SET     CommandType = "SET"
	DEL     CommandType = "DEL"
	GET     CommandType = "GET"
	PERSIST CommandType = "PERSIST"
	INCR    CommandType = "INCR"
	DECR    CommandType = "DECR"
	INCRBY  CommandType = "INCRBY"
	PING    CommandType = "PING"

	COMMAND_DOCS CommandType = "COMMAND"
)

type CommandType string

type Command struct {
	arr *RespArray
}

func (c *Command) Args() []RespType {
	return c.arr.elements
}

type Response struct {
	Data    RespType
	Success bool
}

func CommandFromReader(reader io.Reader) (*Command, error) {
	bufReader := bufio.NewReader(reader)
	line, err := bufReader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	if len(line) <= 2 {
		return nil, fmt.Errorf("invalid protocol")
	}

	if line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("invalid protocol")
	}

	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("invalid protocol")
	}

	numArgs, err := strconv.Atoi(string(bytes.TrimSpace(line[1:])))
	if err != nil {
		return nil, err
	}

	args := &RespArray{}

	for i := 0; i < numArgs; i++ {
		line, err := bufReader.ReadBytes('\n')

		if err != nil {
			return nil, err
		}

		if line[len(line)-2] != '\r' {
			return nil, fmt.Errorf("invalid protocol")
		}

		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("invalid protocol")
		}

		argLen, err := strconv.Atoi(string(bytes.TrimSpace(line[1:])))
		if err != nil {
			return nil, err
		}

		arg := make([]byte, argLen)
		_, err = io.ReadFull(bufReader, arg)
		if err != nil {
			return nil, err
		}

		args.Append(
			&BulkStr{
				Data: arg,
			},
		)

		end, err := bufReader.ReadBytes('\n')

		if err != nil {
			return nil, io.EOF
		}

		if len(end) != 2 || end[0] != '\r' {
			return nil, fmt.Errorf("invalid protocol")
		}
	}

	if args.Length() == 0 {
		return nil, fmt.Errorf("no command")
	}

	return &Command{
		arr: args,
	}, nil
}
