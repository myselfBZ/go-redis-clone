package store

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)

var (
	ErrNotFound  = errors.New("key not found")
	ErrNotInteger = errors.New("value is not an intiger")
)

func NewStorage() *Storage {
	return &Storage{
		mu:   sync.RWMutex{},
		data: make(map[string]resp.RespType),
		janitor: &janitor{
			interval: time.Minute,
			exit:     make(chan struct{}),
		},
		expiringKeys: make(map[string]time.Time),
	}
}

type SetArgs struct {
	Key   string
	Value resp.RespType

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
	data    map[string]resp.RespType
	janitor *janitor

	expiringKeys map[string]time.Time
}

func (s *Storage) StartJanitor() {
	s.janitor.run(s)
}

func (s *Storage) PTTL(key string) int {
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
	ttl := int(math.Round(float64(time.Until(expiresAt).Milliseconds())))
	return ttl

}

func (s *Storage) TTL(key string) int {
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
	ttl := int(math.Round(time.Until(expiresAt).Seconds()))
	return ttl
}

func (s *Storage) Decr(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, exists := s.data[key]
	if !exists || s.deleteIfExpired(key) {
		s.data[key] = &resp.Intiger{
			Data: -1,
		}
		return -1, nil
	}

	if val.Type() != "intiger" {
		return 0, ErrNotInteger 
	}

	intType := val.(*resp.Intiger)
	intType.Data--
	return intType.Data, nil
}

func (s *Storage) IncrBy(key string, by int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, exists := s.data[key]
	if !exists || s.deleteIfExpired(key) {
		s.data[key] = &resp.Intiger{
			Data: by,
		}
		return by, nil
	}

	if val.Type() != "intiger" {
		return 0, ErrNotInteger 
	}

	intType := val.(*resp.Intiger)
	intType.Data += by
	return intType.Data, nil
}

func (s *Storage) Incr(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, exists := s.data[key]
	if !exists || s.deleteIfExpired(key) {
		s.data[key] = &resp.Intiger{
			Data: 1,
		}
		return 1, nil
	}

	if val.Type() != "intiger" {
		return 0, ErrNotInteger 
	}

	intType := val.(*resp.Intiger)
	intType.Data++
	return intType.Data, nil
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
		s.data[args.Key] = args.Value

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

func (s *Storage) Get(key string) (resp.RespType, error) {
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

	return data, nil
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
