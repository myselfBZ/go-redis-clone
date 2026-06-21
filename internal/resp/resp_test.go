package resp

import (
	"bytes"
	"io"
	"slices"
	"testing"
)

// TODO: add more test suites
func TestParse(t *testing.T) {
	data := []RespBulkStrArr{
		{data: [][]byte{[]byte("SET"), []byte("KEY"), []byte("VAL")} },
		{data: [][]byte{[]byte("PING")}},
	}

	r := bytes.NewBuffer([]byte{})

	for _, d := range data {
		r.Write(d.ToBytes())
	}

	ch := Parse(r)
	i := 0

	for c  := range ch {

		if c.Err != nil {
			// end
			if c.Err == io.EOF {
				return
			}

			t.Fatalf("unexpected error. test suite idx %d, error '%v'", i, c.Err)
		}

		expected := data[i].ToBytes()
		got := c.arr.ToBytes()

		if !slices.Equal(expected, got) {
			t.Errorf("expected '%q', got '%q'", string(expected), string(got))
		}
		i++
	}
}
