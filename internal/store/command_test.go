package store

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
	"github.com/myselfBZ/go-redis-clone/pkg/utils"
)

const defaultKeyLength = 5
const defaultValLength = 100

func toBytes(args ...string) [][]byte {
	result := make([][]byte, len(args))
	for i, a := range args {
		result[i] = []byte(a)
	}
	return result
}

type suite struct {
	name string
	expected resp.RespType
	raw [][]byte
}

var testDb = NewStorage()

// TODO: crete a mock db, more test cases
func TestExec(t *testing.T) {
	db := NewStorage()

	tests := []suite {
		{
			name: "GET command",
			raw: [][]byte{[]byte("GET"), []byte("key")},
			expected: &resp.BulkStr{Data: nil},
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
		rep, _ := db.Exec(test.raw)
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

	db.put("key", &dataEntity{
		val: []byte("val"),
	})
	db.expire("key", time.Now().Add(time.Second))

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
	db.put("key", &dataEntity{
		val: []byte("val"),
	})

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
			expected: &resp.BulkStr{Data: nil},
		},
	}
	for _, test := range tests {
		rep := execGet(db, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}
func TestExecDecr(t *testing.T) {
	db := NewStorage()
	nonIntKey := "nonInt"
	nonIntVal := &dataEntity{
		val: []byte("nonInt"),
	}

	db.put(nonIntKey, nonIntVal)

	key := "key"
	val := &dataEntity{
		val: []byte("2"),
	}

	db.put(key, val)

	tests := []suite{
		{
			name: "DECR on non-existent key",
			expected: &resp.Intiger{Data: -1},
			raw: [][]byte{[]byte("key1")},
		},
		{
			name: "DECR on valid key",
			expected: &resp.Intiger{Data: 1},
			raw: [][]byte{[]byte("key")},
		},
		{
			name: "DECR on non-intiger key",
			expected: resp.NotInErr(),
			raw: [][]byte{[]byte(nonIntKey)},
		},
	}

	for _, test := range tests {
		rep := execDecr(db, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
	en, ok := db.get(key)
	if !ok {
		t.Fatalf("fetching the incremented key failed, key does not exist")
	}
	v, ok := en.val.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", en.val)
	}
	intV, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		t.Fatalf("expected valid int, got %s", string(v))
	}
	if intV != 1 {
		t.Fatalf("expected 1, got %d", intV)
	}
}

func TestDecrBy(t *testing.T) {
	db := NewStorage()
	nonIntKey := "nonInt"
	nonIntVal := &dataEntity{
		val: []byte("nonInt"),
	}

	db.put(nonIntKey, nonIntVal)

	key := "key"
	val := &dataEntity{
		val: []byte("1"),
	}

	db.put(key, val)

	tests := []suite{
		{
			name: "DECRBY with negative value",
			expected: &resp.Intiger{Data: 67},
			raw: [][]byte{[]byte("negative"), []byte("-67")},
		},
		{
			name: "DECRBY on non-existent key",
			expected: &resp.Intiger{Data: -67},
			raw: [][]byte{[]byte("key1"), []byte("67")},
		},
		{
			name: "DECRBY on valid key",
			expected: &resp.Intiger{Data: -1},
			raw: [][]byte{[]byte("key"), []byte("2")},
		},
		{
			name: "DECRBY on non-intiger key",
			expected: resp.NotInErr(),
			raw: [][]byte{[]byte(nonIntKey), []byte("15")},
		},
	}

	for _, test := range tests {
		rep := execDecrBy(db, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
	en, ok := db.get(key)
	if !ok {
		t.Fatalf("fetching the incremented key failed, key does not exist")
	}
	v, ok := en.val.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", en.val)
	}
	intV, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		t.Fatalf("expected valid int, got %s", string(v))
	}
	if intV != -1 {
		t.Fatalf("expected -1, got %d", intV)
	}
}

func TestExecIncrBy(t *testing.T) {
	testDb.mu.Lock()
	defer testDb.mu.Unlock()

	nonIntKey := utils.RandString(defaultKeyLength)
	nonIntVal := &dataEntity{
		val: []byte("nonInt"),
	}

	testDb.put(nonIntKey, nonIntVal)

	key := utils.RandString(defaultValLength)
	val := &dataEntity{
		val: []byte("1"),
	}

	testDb.put(key, val)

	tests := []suite{
		{
			name: "INCRBY on non-existent key",
			expected: &resp.Intiger{Data: 67},
			raw: toBytes("does_not_exist", "67"),
		},
		{
			name: "INCRBY on valid key",
			expected: &resp.Intiger{Data: 67},
			raw: toBytes(key, "66"),
		},
		{
			name: "INCRBY on non-intiger key",
			expected: resp.NotInErr(),
			raw: toBytes(nonIntKey, "15"),
		},
	}

	for _, test := range tests {
		rep := execIncrBy(testDb, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
	en, ok := testDb.get(key)
	if !ok {
		t.Fatalf("fetching the incremented key failed, key does not exist")
	}
	v, ok := en.val.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", en.val)
	}
	intV, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		t.Fatalf("expected valid int, got %s", string(v))
	}
	if intV != 67 {
		t.Fatalf("expected 67, got %d", intV)
	}
}

func TestExecIncr(t *testing.T) {
	testDb.mu.Lock()
	defer testDb.mu.Unlock()
	
	nonIntKey := utils.RandString(defaultKeyLength)
	nonIntVal := &dataEntity{
		val: []byte("nonInt"),
	}

	testDb.put(nonIntKey, nonIntVal)

	validKey := utils.RandString(defaultKeyLength)
	val := &dataEntity{
		val: []byte("1"),
	}

	testDb.put(validKey, val)

	tests := []suite{
		{
			name: "INCR on non-existent key",
			expected: &resp.Intiger{Data: 1},
			raw: toBytes("non_existent_key"),
		},
		{
			name: "INCR on valid key",
			expected: &resp.Intiger{Data: 2},
			raw: toBytes(validKey),
		},
		{
			name: "INCR on non-intiger key",
			expected: resp.NotInErr(),
			raw: toBytes(nonIntKey),
		},
	}

	for _, test := range tests {
		rep := execIncr(testDb, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}

func TestExecSet(t *testing.T) {
	testDb.mu.Lock()
	defer testDb.mu.Unlock()

	defKey := utils.RandString(defaultKeyLength)
	defVal := utils.RandString(defaultValLength)

	tests := []suite{
		{
			name: "SET with nx on non-existent key",
			expected: resp.OkReply(),
			raw: toBytes(defKey, defVal, "NX"),
		},
		{
			name: "SET with xx on existing key",
			// previous command set the "key"
			expected: resp.OkReply(),
			raw: toBytes(defKey, defVal, "XX"),
		},
		{
			name: "SET with xx on non-existent key",
			expected: &resp.BulkStr{Data: nil},
			raw: toBytes(defKey+"nonexistent", defVal, "XX"),
		},
		{
			name: "SET with nx on exisiting key",
			// key already exists
			expected: &resp.BulkStr{Data: nil},
			raw: toBytes(defKey, defVal, "NX"),
		},
		{
			name: "SET with malformed ex",
			expected: resp.SyntaxErr(),
			raw: toBytes(defKey, defVal, "EX"),
		},
		{
			name: "SET with ex 10s",
			expected: resp.OkReply(),
			raw: toBytes(defKey, defVal, "EX", "10"),
		},
		{
			name: "SET with px 10ms",
			expected: resp.OkReply(),
			raw: toBytes(defKey, defVal, "PX", "10"),
		},
		{
			name: "SET with malformed px",
			expected: resp.SyntaxErr(),
			raw: toBytes(defKey, defVal, "PX"),
		},
	}
	
	for _, test := range tests {
		rep := execSet(testDb, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}

func TestExecDel(t *testing.T) {
	testDb.mu.Lock()
	defer testDb.mu.Unlock()

	key1, key2, key3 := utils.RandString(5), utils.RandString(5), utils.RandString(5)

	testDb.put(key1, &dataEntity{
		val: []byte("value"),
	})
	testDb.put(key2, &dataEntity{
		val: []byte("value"),
	})
	testDb.put(key3, &dataEntity{
		val: []byte("value"),
	})

	tests := []suite{
		{
			name: "DEL on multiple keys",
			expected: &resp.Intiger{Data: 2},
			raw: toBytes(key1, key2),
		},
		{
			name: "DEL on non-existent key",
			expected: &resp.Intiger{Data: 0},
			raw: toBytes("invalidkey"),
		},
		{
			name: "DEL on existing key",
			expected: &resp.Intiger{Data: 1},
			raw: toBytes(key3),
		},
	}
	
	for _, test := range tests {
		rep := execDel(testDb, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}


func TestExpire(t *testing.T) {
	db := NewStorage()
	db.put("key", &dataEntity{
		val: []byte("val"),
	})

	tests := []suite{
		{
			name: "EXPIRE on exisiting key no options",
			expected: &resp.Intiger{Data: 1},
			raw: [][]byte{[]byte("key"), []byte("10")},
		}, 
		{
			name: "EXPIRE on on non-existent key, no options",
			expected: &resp.Intiger{Data: 0},
			raw: [][]byte{[]byte("key1"), []byte("10")},
		},

		// this should delete the key
		{
			name: "EXPIRE with XX",
			expected: &resp.Intiger{Data: 1},
			raw: [][]byte{[]byte("key"), []byte("0"), []byte("XX")},
		},
	}
	for _, test := range tests {
		rep := execExpire(db, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
	_, ok := db.get("key")
	if ok {
		t.Fatalf("EXPIRE with XX did not delete the key")
	}

	db.put("key", &dataEntity{
		val: []byte("val"),
	})

	testWithOptions := []suite{
		{
			name: "EXPIRE with NX",
			expected: &resp.Intiger{Data: 1},
			raw: [][]byte{[]byte("key"), []byte("10"), []byte("NX")},
		},
	}

	for _, test := range testWithOptions {
		rep := execExpire(db, test.raw)
		if !slices.Equal(rep.ToBytes(), test.expected.ToBytes()) {
			t.Fatalf("%s. Response did not match. got '%q', want '%q'", test.name, string(rep.ToBytes()), string(test.expected.ToBytes()))
		}
	}
}
