package resp

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type chunkReader struct {
	data            string
	numBytesPerRead int
	pos             int
}

func (cr *chunkReader) Read(p []byte) (n int, err error) {

	if cr.pos >= len(cr.data) {
		return 0, io.EOF
	}

	readEndIdx := cr.pos + cr.numBytesPerRead
	readEndIdx = min(readEndIdx, len(cr.data))
	n = copy(p, cr.data[cr.pos:readEndIdx])
	cr.pos += n

	return n, nil
}

func TestParseCommand(t *testing.T) {
	reader := &chunkReader{
		data: "*0\r\n",
		numBytesPerRead: 1,
	}

	command, err := CommandFromReader(reader)
	// i mean... yeah it is hacky
	require.NotNil(t, err)

	reader = &chunkReader{
		data: "*sd\r\n",
		numBytesPerRead: 1,
	}

	command, err = CommandFromReader(reader)

	require.Error(t, err)
	require.Nil(t, command)

	reader = &chunkReader{
		data: "*1\r\n$3\r\nSET\r\n",
		numBytesPerRead: 1,
	}

	command, err = CommandFromReader(reader)

	require.NoError(t, err)
	
	require.Equal(t, 1,command.arr.Length())

	bulkStr, ok := command.arr.elements[0].(*BulkStr)
	require.True(t, ok)
	require.Equal(t, "SET", bulkStr.Data)


	reader = &chunkReader{
		data: "*1\r\n$3\r\nSETTLE\r\n",
		numBytesPerRead: 1,
	}

	command, err = CommandFromReader(reader)
	require.Error(t, err)
}
