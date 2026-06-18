package store

import (
	"slices"
	"testing"
	"time"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)

type suite struct {
	name string
	expected resp.RespType
	raw [][]byte
}



// TODO: crete a mock db, more test cases
func TestExec(t *testing.T) {
	db := NewStorage()

	tests := []suite {
		{
			name: "GET command",
			raw: [][]byte{[]byte("GET"), []byte("key")},
			expected: &resp.Nil{},
		},
		{
			name: "SET command | invalid args",
			raw: [][]byte{[]byte("SET"), []byte("key1")},
			expected: resp.ArgNumErr("set"),
		},
		{
			name: "SET command | valid args",
			raw: [][]byte{[]byte("SET"), []byte("key1"), []byte("value1")},
			expected: resp.OkReply(),
		},
	}


	for _, test := range tests {
		rep := db.Exec(test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}
func TestPersist(t *testing.T) {
	db := NewStorage()
	raw := [][]byte{
		[]byte("SET"), 
		[]byte("key"),
		[]byte("val"),
		[]byte("EX"),
		[]byte("1"),
	}

	db.Exec(raw)

	res := execPersist(db, [][]byte{[]byte("key")})
	exp := &resp.Intiger{Data: 1}	
	if !slices.Equal(res.ToBytes(), exp.ToBytes()) {
		t.Fatalf("Response did not match. got '%q', want '%q'", string(res.ToBytes()), string(exp.ToBytes()))
	}

	// expiration
	time.Sleep(time.Second)

	res = execPersist(db, [][]byte{[]byte("key")})
	exp = &resp.Intiger{Data: 0}	
	if !slices.Equal(res.ToBytes(), exp.ToBytes()) {
		t.Fatalf("Response did not match. got '%q', want '%q'", string(res.ToBytes()), string(exp.ToBytes()))
	}


	// persisting a key that does not have a TTL
	raw = [][]byte{
		[]byte("SET"), 
		[]byte("key"),
		[]byte("val"),
		[]byte("ex"),
	}

	db.Exec(raw)

	res = execPersist(db, [][]byte{[]byte("key")})
	exp = &resp.Intiger{Data: 0}	
	if !slices.Equal(res.ToBytes(), exp.ToBytes()) {
		t.Fatalf("Response did not match. got '%q', want '%q'", string(res.ToBytes()), string(exp.ToBytes()))
	}

}

func TestTTL(t *testing.T) {
	db := NewStorage()
	raw := [][]byte{
		[]byte("SET"), 
		[]byte("key"),
		[]byte("val"),
		[]byte("EX"),
		[]byte("1"),
	}

	db.Exec(raw)

	res := execTtl(db, [][]byte{[]byte("key")})
	exp := &resp.Intiger{Data: 1}	
	if !slices.Equal(res.ToBytes(), exp.ToBytes()) {
		t.Fatalf("Response did not match. got '%q', want '%q'", string(res.ToBytes()), string(exp.ToBytes()))
	}

	// expiration
	time.Sleep(time.Second)

	res = execTtl(db, [][]byte{[]byte("key")})
	exp = &resp.Intiger{Data: -2}	
	if !slices.Equal(res.ToBytes(), exp.ToBytes()) {
		t.Fatalf("Response did not match. got '%q', want '%q'", string(res.ToBytes()), string(exp.ToBytes()))
	}


	// expecting -1
	raw = [][]byte{
		[]byte("SET"), 
		[]byte("key"),
		[]byte("val"),
	}

	db.Exec(raw)

	res = execTtl(db, [][]byte{[]byte("key")})
	exp = &resp.Intiger{Data: -1}	
	if !slices.Equal(res.ToBytes(), exp.ToBytes()) {
		t.Fatalf("Response did not match. got '%q', want '%q'", string(res.ToBytes()), string(exp.ToBytes()))
	}

}

func TestGet(t *testing.T) {
	db := NewStorage()

	db.put("hello", &dataEntity{
		val: []byte("world"),
	})

	tests := []suite{
		{
			name: "GET exisiting key",
			raw: [][]byte{[]byte("hello")},
			expected: &resp.BulkStr{
				Data: []byte("world"),
			},

		},
		{
			name: "GET non-existent key",
			raw: [][]byte{[]byte("key")},
			expected: &resp.Nil{},
		},
	}
	for _, test := range tests {
		rep := execGet(db, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}

func TestExecSet(t *testing.T) {
	db := NewStorage()

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
			name: "SET with xx on non-existent key",
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
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}

func TestExecDel(t *testing.T) {
	db := NewStorage()
	db.put("key", &dataEntity{
		val: []byte("value"),
	})
	db.put("key1", &dataEntity{
		val: []byte("value"),
	})
	db.put("key2", &dataEntity{
		val: []byte("value"),
	})

	tests := []suite{
		{
			name: "DEL on multiple keys",
			expected: &resp.Intiger{Data: 2},
			raw: [][]byte{[]byte("key1"), []byte("key2")},
		},
		{
			name: "DEL on non-existent key",
			expected: &resp.Intiger{Data: 0},
			raw: [][]byte{[]byte("invalid")},
		},
		{
			name: "DEL on existing key",
			expected: &resp.Intiger{Data: 1},
			raw: [][]byte{[]byte("key")},
		},
	}
	
	for _, test := range tests {
		rep := execDel(db, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}
