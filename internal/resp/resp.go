package resp

import (
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

type CommandType string

const (
	SET          CommandType = "SET"
	DEL          CommandType = "DELETE"
	GET          CommandType = "GET"
	COMMAND_DOCS CommandType = "COMMAND"
)

type commandState string

const (
	stateInit     commandState = "init"
	stateElements commandState = "elements"
	stateDone     commandState = "done"
)

type Command struct {
	state commandState
	arr   *RespArray
}

func (c *Command) parse(data []byte) (int, error) {
	read := 0
outer:
	for {
		switch c.state {
		case stateInit:
			commandArr, err := parseArray(data)

			if err != nil {
				return 0, err
			}

			if commandArr == nil {
				break outer
			}
			read += 2 + len(CRLF)
			c.arr = commandArr
			c.state = stateElements
		case stateElements:
			for len(c.arr.elements) < c.arr.Length() {
				bulkStr, n, err := parseBulkStr(data[read:])
				if err != nil {
					return 0, err
				}

				if n == 0 {
					break outer
				}

				read += n

				c.arr.Append(bulkStr)
			}

			c.state = stateDone
		case stateDone:
			break outer
		}
	}

	return read, nil

}

func (c *Command) done() bool {
	return c.state == stateDone
}

func (c *Command) Args() []RespType {
	return c.arr.elements
}

func (c *Command) ArgsString() string {
	msg := ""
	for _, e := range c.arr.elements {

		switch arg := e.(type) {
		case *BulkStr:
			msg += arg.Data + "\r\n"
		default:
			panic("Woopsie some how i messed it up")
		}

	}

	return msg
}

func CommandFromReader(reader io.Reader) (*Command, error) {
	command := &Command{
		state: stateInit,
		arr:   &RespArray{},
	}
	buff := make([]byte, 1024)
	buffLen := 0
	for !command.done() {
		n, err := reader.Read(buff[buffLen:])
		if err != nil {
			if err == io.EOF {
				return command, err
			}
		}

		buffLen += n

		readN, err := command.parse(buff[:buffLen])

		if err != nil {
			return nil, err
		}

		copy(buff, buff[readN:buffLen])
		buffLen -= readN
	}

	return command, nil
}

func parseArray(data []byte) (*RespArray, error) {
	idx := bytes.Index(data, CRLF)

	if idx < 0 {
		return nil, nil
	}

	arrData := data[:idx]
	length, err := strconv.Atoi(string(arrData[1]))
	if err != nil {
		return nil, errors.Join(ErrInvalidValue, fmt.Errorf("expected int, got: %v", arrData[1]))
	}

	return &RespArray{
		length: length,
	}, nil
}

func parseBulkStr(data []byte) (*BulkStr, int, error) {
	read := 0

	idx := bytes.Index(data, CRLF)
	if idx < 0 {
		return nil, 0, nil
	}
	bulkStrBytes := data[:idx]
	read += len(data[:idx]) + len(CRLF)

	if string(bulkStrBytes[0]) != "$" {
		return nil, 0, errors.Join(ErrInvalidType, fmt.Errorf("expected '$' for bulk string. got: %s", string(bulkStrBytes[0])))
	}

	lenght, err := strconv.Atoi(string(bulkStrBytes[1]))
	if err != nil {
		return nil, 0, errors.Join(ErrInvalidValue, fmt.Errorf("expected int, got: %v", bulkStrBytes[1]))
	}

	bulkStrEnd := bytes.Index(data[idx+len(CRLF):], CRLF)
	// see if the \r\n is there
	if bulkStrEnd < 0 {
		// ... but if it is not there, check
		// if the data hasn't exceeded the defined length
		if len(data[idx+len(CRLF):]) > 0 && isValidBulkStrLength(lenght, data[idx+len(CRLF):]) {
			return nil, 0, errors.Join(ErrInvalidLength, fmt.Errorf("expected %d for bulk string, got more than %d", lenght, lenght))
		}

		return nil, 0, nil
	}

	bulkStr := data[read : read+lenght]

	read += len(CRLF) + lenght
	return &BulkStr{
		Data: string(bulkStr),
	}, read, nil
}

func isValidBulkStrLength(length int, data []byte) bool {
	actualDataLength := len(data)
	lastByte := data[len(data)-1]
	if string(lastByte) == "\r" {
		actualDataLength -= 1
	}
	return actualDataLength > length
}
