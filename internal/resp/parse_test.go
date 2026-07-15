package resp

import (
	"bytes"
	"testing"
)

type chunkReader struct {
	data            string
	numBytesPerRead int
	pos             int
}

func (cr *chunkReader) Read(p []byte) (n int, err error) {

	if cr.pos >= len(cr.data) {
		return 0, nil
	}

	readEndIdx := cr.pos + cr.numBytesPerRead
	readEndIdx = min(readEndIdx, len(cr.data))
	n = copy(p, cr.data[cr.pos:readEndIdx])
	cr.pos += n

	return n, nil
}

func TestParse0(t *testing.T) {
	r := &chunkReader{
		data: "*3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$3\r\nVAL\r\n",
		numBytesPerRead: 4,
	}

	ch := Parse0(r)

	cmd := <- ch

	if cmd.Err != nil  {
		t.Errorf("unexpected error: %v", cmd.Err)
		return
	}

	if cmd.arr.Length() != 3 {
		t.Errorf("expected array length of 3, got %d", cmd.arr.Length())
	}

	if !bytes.Equal(cmd.arr.data[0], []byte("SET")) {
		t.Errorf("first element expected 'SET', got %s", string(cmd.arr.data[0]))
	}


	if !bytes.Equal(cmd.arr.data[1], []byte("KEY")) {
		t.Errorf("second element expected 'KEY', got %s", string(cmd.arr.data[1]))
	}


	r = &chunkReader{
		data: "*1\r\n$3\r\nSET\r\n",
		numBytesPerRead: 4,
	}

	ch = Parse0(r)
	cmd = <- ch

	if cmd.Err != nil  {
		t.Errorf("unexpected error: %v", cmd.Err)
		return
	}

	if cmd.arr.Length() != 1 {
		t.Errorf("expected array length of 3, got %d", cmd.arr.Length())
	}

	if !bytes.Equal(cmd.arr.data[0], []byte("SET")) {
		t.Errorf("first element expected 'SET', got %s", string(cmd.arr.data[0]))
	}
}
