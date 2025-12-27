package resp


var (
	ARRAY = []byte("*")
	BULKSTR = []byte("$")
)

type RespType interface{
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
	Data string
}

func (rt *BulkStr) Type() string {
	return "bulkstr"
}

type Intiger struct {
	Data int
}

func (rt *Intiger) Type() string {
	return "intiger"
}

