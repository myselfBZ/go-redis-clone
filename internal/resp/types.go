package resp

import (
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
	_ RespType = (*RespArray)(nil)
	_ RespType = (*RespErr)(nil)
	_ RespType = (*BulkStr)(nil)
	_ RespType = (*Nil)(nil)
	_ RespType = (*Intiger)(nil)
)

type RespType interface {
	Type() string
	ToBytes() []byte
}

type RespArray struct {
	elements []RespType
}

func (rt *RespArray) ToBytes() []byte {
	// *len \r\n [elements] \r\n
	buff := []byte{}
	buff = append(buff, '*')
	buff = append(buff, []byte(strconv.Itoa(len(rt.elements)))...)
	buff = append(buff, CRLF...)
	for _, r := range rt.elements {
		buff = append(buff, r.ToBytes()...)
	}
	buff = append(buff, CRLF...)
	return buff
}

func (rt *RespArray) Append(e RespType) {
	rt.elements = append(rt.elements, e)
}

func (rt *RespArray) Length() int {
	return len(rt.elements)
}

func (rt *RespArray) Type() string {
	return "array"
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

type Nil struct {}

func (rt *Nil) ToBytes() []byte {
	return []byte("_\r\n")
}

func (rt *Nil) Type() string {
	return "(nil)"
}



