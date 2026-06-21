package resp

import (
	"bytes"
	"fmt"
	"strconv"
)

var (
	ARRAY     = []byte("*")
	BULKSTR   = []byte("$")
	SIMPLESTR = []byte("+")
	NULLBULKSTR = []byte("$-1\r\n")
)

var(
	_ RespType = (*RespBulkStrArr)(nil)
	_ RespType = (*RespErr)(nil)
	_ RespType = (*BulkStr)(nil)
	_ RespType = (*Intiger)(nil)
)

type RespType interface {
	Type() string
	ToBytes() []byte
}

type RespBulkStrArr struct {
	data [][]byte
}

func (rt *RespBulkStrArr) Append(e []byte) {
	rt.data = append(rt.data, e)
}

func (rt *RespBulkStrArr) Length() int {
	return len(rt.data)
}

func (rt *RespBulkStrArr) Type() string {
	return "bulk_str_array"
}

func (rt *RespBulkStrArr) ToBytes() []byte {
	if len(rt.data) == 0 {
		return []byte("*0\r\n")
	}

	var buf bytes.Buffer
	argLen := len(rt.data)
	bufLen := 1 + len(strconv.Itoa(argLen)) + 2
	for _, arg := range rt.data {
		if arg == nil {
			bufLen += 3 + 2
		} else {
			bufLen += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2
		}
	}
	buf.Grow(bufLen)
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(argLen))
	buf.WriteString(string(CRLF))
	for _, arg := range rt.data {
		if arg == nil {
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		} else {
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(len(arg)))
			buf.WriteString(CRLF)
			buf.Write(arg)
			buf.WriteString(CRLF)
		}
	}
	return buf.Bytes()
}

type BulkStr struct {
	Data []byte
}

func (rt *BulkStr) Type() string {
	return "bulkstr"
}

func (r *BulkStr) ToBytes() []byte {
    if r.Data == nil {
        return NULLBULKSTR
    }
	// $ + int64 + CRLF + payload + CRLF
    buf := make([]byte, 0, 1+20+2+len(r.Data)+2)
    buf = append(buf, '$')
    buf = strconv.AppendInt(buf, int64(len(r.Data)), 10)
    buf = append(buf, '\r', '\n')
    buf = append(buf, r.Data...)
    buf = append(buf, '\r', '\n')
    return buf
}

func (rt *BulkStr) String() string {
	return string(rt.Data)
}

type Intiger struct {
	Data int64
}

func (rt *Intiger) Type() string {
	return "intiger"
}

func (rt *Intiger) ToBytes() []byte {
	return []byte(fmt.Sprintf(":%d\r\n", rt.Data))
}

type SimpleStr struct {
	Data []byte
}

func (rt *SimpleStr) Type() string {
	return "simple"
}

func (rt *SimpleStr) ToBytes() []byte {
	return []byte("+" + string(rt.Data) + string(CRLF))
}

type RespErr struct {
	Data []byte
}

func (rt *RespErr) ToBytes() []byte {
	return []byte("-" + string(rt.Data) + string(CRLF))
}

func (rt *RespErr) Type() string {
	return "error"
}
