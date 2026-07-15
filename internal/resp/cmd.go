package resp

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
