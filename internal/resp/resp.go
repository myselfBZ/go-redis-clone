package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)


var (
	CRLF = "\r\n"
)


type Command struct {
	arr *RespBulkStrArr
	Err error
}

func (c *Command) DebugStr() string {
	s := ""
	for _, c := range c.arr.data {
		s += string(c) + " "
	}
	return s
}

func (c *Command) Args() [][]byte {
	return c.arr.data
}


func parse(bufReader *bufio.Reader, ch chan <- *Command) {
	defer close(ch)

outer:
	for {
		cmd := &Command{}

		line, err := bufReader.ReadBytes('\n')

		if err != nil {
			cmd.Err = err
			ch <- cmd
			return
		}

		if len(line) <= 2 {
			cmd.Err = fmt.Errorf("invalid protocol")
			ch <- cmd
			continue
		}

		if line[len(line)-2] != '\r' {
			cmd.Err = fmt.Errorf("invalid protocol")
			ch <- cmd
			continue
		}

		if len(line) == 0 || line[0] != '*' {
			cmd.Err = fmt.Errorf("invalid protocol")
			ch <- cmd
			continue
		}

		numArgs, err := strconv.Atoi(string(bytes.TrimSpace(line[1:])))
		if err != nil {
			cmd.Err = err
			ch <- cmd
			continue
		}

		args := &RespBulkStrArr{}

		for i := 0; i < numArgs; i++ {
			line, err := bufReader.ReadBytes('\n')

			if err != nil {
				cmd.Err = err
				ch <- cmd
				return
			}

			if line[len(line)-2] != '\r' {
				cmd.Err = fmt.Errorf("invalid protocol")
				ch <- cmd
				continue outer
			}

			if len(line) == 0 || line[0] != '$' {
				cmd.Err = fmt.Errorf("invalid protocol")
				ch <- cmd
				continue outer

			}

			argLen, err := strconv.Atoi(string(bytes.TrimSpace(line[1:])))
			if err != nil {
				cmd.Err = err
				ch <- cmd
				return
			}

			arg := make([]byte, argLen)
			_, err = io.ReadFull(bufReader, arg)

			if err != nil {
				cmd.Err = err
				ch <- cmd
				return
			}

			args.Append(arg)

			end, err := bufReader.ReadBytes('\n')

			if err != nil {
				cmd.Err = err
				ch <- cmd
				return	
			}

			if len(end) != 2 || end[0] != '\r' {
				cmd.Err = fmt.Errorf("invalid protocol")
				ch <- cmd
				continue
			}
		}

		if args.Length() == 0 {
			cmd.Err = fmt.Errorf("no command")
			ch <- cmd
			continue outer
		}

		cmd.arr = args
		ch <- cmd

	}
}

func Parse(reader io.Reader) (chan *Command) {
	ch := make(chan *Command)
	bufReader := bufio.NewReader(reader)
	go parse(bufReader, ch)
	return ch
}
