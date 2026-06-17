package store

import (
	"slices"
	"testing"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)

func TestExecSet(t *testing.T) {
	db := NewStorage()
	type suite struct {
		name string
		expected resp.RespType
		raw [][]byte
	}

	tests := []suite{
		{
			name: "SET with nx on non-existent key",
			expected: resp.OkReply(),
			raw: [][]byte{[]byte("key"), []byte("val"), []byte("nx")},
		},
		{
			name: "SET with xx on existing key",
			// previous command set the "key"
			expected: resp.OkReply(),
			raw: [][]byte{[]byte("key"), []byte("val"), []byte("xx")},
		},
		{
			name: "SET with xx on non-exisitent key",
			expected: &resp.Nil{},
			raw: [][]byte{[]byte("key1"), []byte("val"), []byte("xx")},
		},
		{
			name: "SET with nx on exisiting key",
			// key already exists
			expected: &resp.Nil{},
			raw: [][]byte{[]byte("key"), []byte("val"), []byte("nx")},
		},
		{
			name: "SET with malformed ex",
			expected: resp.SyntaxErr(),
			raw: [][]byte{[]byte("key"), []byte("val"), []byte("ex")},
		},
		{
			name: "SET with ex 10s",
			expected: resp.OkReply(),
			raw: [][]byte{[]byte("key"), []byte("val"), []byte("ex"), []byte("10")},
		},
		{
			name: "SET with px 10ms",
			expected: resp.OkReply(),
			raw: [][]byte{[]byte("key"), []byte("val"), []byte("px"), []byte("10")},
		},
		{
			name: "SET with malformed px",
			expected: resp.SyntaxErr(),
			raw: [][]byte{[]byte("key"), []byte("val"), []byte("px")},
		},
	}
	
	for _, test := range tests {
		rep := execSet(db, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}
