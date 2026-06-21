package store

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)

var(
	ErrClosed = fmt.Errorf("use of closed storage engine")
)

// it's for the command executors, for now...
type kVStore interface {
	put(key string, val *dataEntity) int
	remove(key string) int
	putIfExists(key string, val *dataEntity) int
	expire(key string, at time.Time) int
	persist(key string) int
	putIfAbsent(key string, val *dataEntity) int
	getExpiresAt(key string) (time.Time, bool)
	get(key string) (*dataEntity, bool)
}

func NewStorage() *Storage {
	s := &Storage{
		mu:   sync.RWMutex{},
		data: make(map[string]*dataEntity),
		closed: false,
		janitor: &janitor{
			interval: time.Minute,
			exit:     make(chan struct{}),
		},
		expiringKeys: make(map[string]time.Time),
	}
	go s.startJanitor()
	return s
}


type janitor struct {
	interval time.Duration
	exit     chan struct{}
}

func (j *janitor) run(s *Storage) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.deleteExpired()
		case <-j.exit:
			return
		}
	}
}

type Storage struct {
	mu      sync.RWMutex
	data    map[string]*dataEntity
	janitor *janitor
	//accessed by only .Close() and only  .Close()
	closed 	bool

	expiringKeys map[string]time.Time
}


// Exec executes the given command. It returns an error ONLY when it's called after the storage is closed.
// Returned error is ALWAYS ErrClosed.
func (s *Storage) Exec(cmd [][]byte) (resp.RespType, error) {
	if s.closed {
		return nil, ErrClosed
	}

	name := strings.ToLower(string(cmd[0]))
	c, ok := cmdTable[name]
	if !ok {
		return resp.MakeErr("ERR invalid command"), nil
	}

	if !validArity(c.arity, len(cmd)) {
		return resp.ArgNumErr(name), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return c.exec(s, cmd[1:]), nil
}

func (s *Storage) Close() {
	s.closed = true
	close(s.janitor.exit)
	s.mu.Lock()
	// clearing up the map
	s.data = make(map[string]*dataEntity)
	s.data = make(map[string]*dataEntity)
	s.mu.Unlock()
}

func (s *Storage) startJanitor() {
	s.janitor.run(s)
}

func (s *Storage) deleteExpired() {
	expiredKeys := []string{}
	s.mu.Lock()
	for k, expiresAt := range s.expiringKeys {
		if time.Now().After(expiresAt) {
			expiredKeys = append(expiredKeys, k)
		}
	}
	s.mu.Unlock()

	if len(expiredKeys) > 0 {
		s.mu.Lock()
		for _, key := range expiredKeys {
			s.deleteKey(key)
		}
		s.mu.Unlock()
	}
}

func (s *Storage) deleteKey(key string) {
	delete(s.data, key)
	delete(s.expiringKeys, key)
}

func (s *Storage) deleteIfExpired(key string) {
	expiresAt, ok := s.expiringKeys[key]

	if !ok {
		return
	}

	if time.Now().After(expiresAt) {
		s.deleteKey(key)
	}
}

// -------- basic ops ---------

func (s *Storage) exists(key string) bool {
	_, ok := s.data[key]
	return ok
}

func (s *Storage) get(key string) (*dataEntity, bool) {
	s.deleteIfExpired(key)
	en, ok := s.data[key]
	return en, ok
}

func (s *Storage) put(key string, val *dataEntity) int {
	s.data[key] = val
	return 1
}

// putIfExists updates the existing key and returns 1, if the key does not exist
// it returns 0 and does nothing
func (s *Storage) putIfExists(key string, val *dataEntity) int {
	if !s.exists(key) {
		return 0
	}
	_ = s.put(key, val)
	return 1
}

// putIfAbsent inserts a new key and returns 1, if the key already exists,
// it returns 0 and does nothing
func (s *Storage) putIfAbsent(key string, val *dataEntity) int {
	if s.exists(key) {
		return 0
	}
	_ = s.put(key, val)
	return 1
}

func (s *Storage) getExpiresAt(key string) (time.Time, bool) {
	t, ok := s.expiringKeys[key]
	return t, ok
}

// remove deletes a key, it retuns 1 on success and 0 if the key does not exist 
func (s *Storage) remove(key string) int {
	s.deleteIfExpired(key)

	if !s.exists(key) {
		return 0
	}

	delete(s.data, key)
	return 1
}

// persist deletes the key from the expiringKeys map
// retuns 1 if the key has expiration, 0 if it does not
func (s *Storage) persist(key string) int {
	s.deleteIfExpired(key)

	if !s.exists(key) {
		return 0
	}

	_, ok := s.expiringKeys[key]
	if !ok {
		return 0
	}
	delete(s.expiringKeys, key)
	return 1
}

// expire sets an expiration on the specified key,
// returns 1 if the key exists, 0 if it does not
func (s *Storage) expire(key string, at time.Time) int {
	s.deleteIfExpired(key)

	if !s.exists(key) {
		return 0
	}
	s.expiringKeys[key] = at
	return 1
}
