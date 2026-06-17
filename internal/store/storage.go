package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotFound  = errors.New("key not found")
	ErrNotInteger = errors.New("value is not an intiger")
)

func NewStorage() *Storage {
	s := &Storage{
		mu:   sync.RWMutex{},
		data: make(map[string]*dataEntity),
		janitor: &janitor{
			interval: time.Minute,
			exit:     make(chan struct{}),
		},
		expiringKeys: make(map[string]time.Time),
	}
	go s.startJanitor()
	return s
}

type SetArgs struct {
	Key   string
	Value interface{}

	PX int // ms
	EX int // seconds
	XX bool
	NX bool
}

type ExpireArgs struct {
	Key     string
	Seconds int

	XX, NX bool
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

	expiringKeys map[string]time.Time
}

func (s *Storage) Close() {
	s.janitor.exit <- struct{}{}
}

func (s *Storage) startJanitor() {
	s.janitor.run(s)
}

func (s *Storage) PTTL(key string) int64 {
	s.mu.RLock()
	expiresAt, hasExpire := s.expiringKeys[key]
	_, existsInCache := s.data[key]
	s.mu.RUnlock()

	if !hasExpire && !existsInCache {
		return -2
	}	

	if !hasExpire && existsInCache {
		return -1
	}

	if s.deleteIfExpired(key) {
		return -2
	}
	ttl := int64(math.Round(float64(time.Until(expiresAt).Milliseconds())))
	return ttl

}

func (s *Storage) TTL(key string) int64 {
	s.mu.RLock()
	expiresAt, hasExpire := s.expiringKeys[key]
	_, existsInCache := s.data[key]
	s.mu.RUnlock()

	if !hasExpire && !existsInCache {
		return -2
	}

	if !hasExpire && existsInCache {
		return -1
	}

	if s.deleteIfExpired(key) {
		return -2
	}
	ttl := int64(math.Round(time.Until(expiresAt).Seconds()))
	return ttl
}

func (s *Storage) Decr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entity, exists := s.data[key]
	if !exists || s.deleteIfExpired(key) {
		s.data[key] = &dataEntity{
			val: -1,
		}
		return -1, nil
	}

	val := entity.val.([]byte)

	validInt, err := strconv.ParseInt(string(val), 10, 64)
	
	if err != nil {
		return 0, ErrNotInteger
	}

	validInt--

	entity.val = []byte(strconv.FormatInt(validInt, 10))

	return validInt, nil
}

func (s *Storage) IncrBy(key string, by int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entity, exists := s.data[key]
	if !exists || s.deleteIfExpired(key) {
		s.data[key] = &dataEntity{
			val: by,
		}
		return by, nil
	}
	val := entity.val.([]byte)

	validInt, err := strconv.ParseInt(string(val), 10, 64)
	
	if err != nil {
		return 0, ErrNotInteger
	}

	validInt += by

	entity.val = []byte(strconv.FormatInt(validInt, 10))

	return validInt, nil
}

func (s *Storage) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entity, exists := s.data[key]
	if !exists || s.deleteIfExpired(key) {
		s.data[key] = &dataEntity{
			val: 1,
		}
		return 1, nil
	}

	val := entity.val.([]byte)

	validInt, err := strconv.ParseInt(string(val), 10, 64)
	
	if err != nil {
		return 0, ErrNotInteger
	}

	validInt++

	entity.val = []byte(strconv.FormatInt(validInt, 10))

	return validInt, nil
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

func (s *Storage) Expire(args ExpireArgs) bool {
	now := time.Now()
	expiresAt := now.Add(time.Duration(args.Seconds) * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()
	_, hasExpiry := s.expiringKeys[args.Key]
	_, exists := s.data[args.Key]

	if !exists {
		return false
	}

	if hasExpiry && s.deleteIfExpired(args.Key) {
		return false
	}

	if args.Seconds <= 0 {
		s.deleteKey(args.Key)
		return exists
	}

	if hasExpiry && args.NX {
		return false
	}

	if !args.XX && !args.NX {
		s.expiringKeys[args.Key] = expiresAt
		return true
	}

	if args.XX && exists && hasExpiry {
		s.expiringKeys[args.Key] = expiresAt
		return true
	}

	if args.NX && !hasExpiry && exists {
		s.expiringKeys[args.Key] = expiresAt
		return true
	}

	return false
}

func (s *Storage) Set(args SetArgs) bool {
	written := false

	s.mu.Lock()
	_, ok := s.data[args.Key]
	_, hasExpiration := s.expiringKeys[args.Key]

	if (ok && args.XX) || (!ok && args.NX) || (!args.XX && !args.NX) {
		s.data[args.Key] = &dataEntity{
			val: args.Value,
		}


		if args.EX > 0 {
			expiresAt := time.Now().Add(time.Duration(args.EX) * time.Second)
			s.expiringKeys[args.Key] = expiresAt
		}

		if args.PX > 0 {
			expiresAt := time.Now().Add(time.Duration(args.PX) * time.Millisecond)
			s.expiringKeys[args.Key] = expiresAt
		}

		if args.PX == 0 && args.EX == 0 && hasExpiration{
			delete(s.expiringKeys, args.Key)
		}

		written = true
	}
	s.mu.Unlock()
	return written
}

func (s *Storage) Get(key string) ([]byte, error) {
	s.mu.Lock()
	if s.deleteIfExpired(key) {
		s.mu.Unlock()
		return nil, ErrNotFound
	}
	data, ok := s.data[key]
	s.mu.Unlock()

	if !ok {
		return nil, ErrNotFound
	}

	val := data.val.([]byte)
	return val, nil
}

func (s *Storage) Del(key string) error {
	defer s.mu.Unlock()
	s.mu.Lock()
	_, ok := s.data[key]
	if !ok {
		return ErrNotFound
	}
	s.deleteKey(key)
	return nil
}


func (s *Storage) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.data[key]
	_, hasTTL := s.expiringKeys[key]

	if !exists || !hasTTL {
		return false
	}

	// already expired
	if hasTTL && s.deleteIfExpired(key){
		return false
	}

	delete(s.expiringKeys, key)
	return true
}

// internal-only under lock
func (s *Storage) deleteKey(key string) {
	delete(s.data, key)
	delete(s.expiringKeys, key)
}

// under lock only
func (s *Storage) deleteIfExpired(key string) bool {
	expiresAt, ok := s.expiringKeys[key]

	if !ok {
		return false
	}

	if time.Now().After(expiresAt) {
		delete(s.data, key)
		delete(s.expiringKeys, key)
		return true
	}
	return false
}


// -------- basic ops ---------

func (s *Storage) exists(key string) bool {
	_, ok := s.data[key]
	return ok
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


func (s *Storage) expire(key string, at time.Time) {
	if !s.exists(key) {
		panic("expire() called on nonexistent key " + key)
	}
	s.expiringKeys[key] = at
}

