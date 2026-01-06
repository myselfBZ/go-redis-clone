package resp

var (
	ARRAY     = []byte("*")
	BULKSTR   = []byte("$")
	SIMPLESTR = []byte("+")
)

type RespType interface {
	Type() string
}

type RespArray struct {
	elements []RespType
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

func (rt *BulkStr) String() string {
	return string(rt.Data)
}

type Intiger struct {
	Data int
}

func (rt *Intiger) Type() string {
	return "intiger"
}

type SimpleStr struct {
	Data []byte
}

func (rt *SimpleStr) Type() string {
	return "simple"
}

type RespErr struct {
	Data []byte
}

func (rt *RespErr) Type() string {
	return "error"
}

type Nil struct {}

func (rt *Nil) Type() string {
	return "(nil)"
}



