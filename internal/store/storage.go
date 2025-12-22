package store

import (
	"errors"
	"sync"
	"time"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)

var (
	ErrNotFound = errors.New("key not found")
)

func NewStorage() *Storage {
	return &Storage{
		mu:   &sync.RWMutex{},
		data: make(map[string]resp.RespType),
		janitor: &janitor{
			interval: time.Minute,
			exit: make(chan struct{}),
		},
		expiringKeys: make(map[string]time.Time),
	}
}

type SetArgs struct {
	Key   string
	Value resp.RespType

	PX    int // ms
	EX    int // seconds
	XX    bool
	NX    bool
}

type janitor struct {
	interval time.Duration
	exit chan struct {}
}

func (j *janitor) run(s *Storage) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.DeleteExpired()
		case <-j.exit:
			return
		}
	}
}


type Storage struct {
	mu   *sync.RWMutex
	data map[string]resp.RespType
	janitor *janitor 

	expiringKeys map[string]time.Time
}

func (s *Storage) StartJanitor() {
	s.janitor.run(s)
}

func (s *Storage) DeleteExpired() {
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
			delete(s.data, key)
			delete(s.expiringKeys, key)
		}
		s.mu.Unlock()
	}
}

func (s *Storage) Set(args SetArgs) error {

	if args.EX > 0 && args.PX > 0 {
		return errors.New("EX and PX can't have non-zero value at a time")
	}

	if args.XX && args.NX {
		return errors.New("XX and NX can't have non-zero value at a time")
	}

	s.mu.Lock()
	_, ok := s.data[args.Key]

	if 	(ok && args.XX) || (!ok && args.NX)  || (!args.XX && !args.NX) {
		s.data[args.Key] = args.Value

		if args.EX > 0 {
			expiresAt := time.Now().Add(time.Duration(args.EX) * time.Second)
			s.expiringKeys[args.Key] = expiresAt
		}

		if args.PX > 0 {
			expiresAt := time.Now().Add(time.Duration(args.PX) * time.Millisecond)
			s.expiringKeys[args.Key] = expiresAt 
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *Storage) Get(key string) (resp.RespType, error) {
	s.mu.Lock()
	expiresAt, ok := s.expiringKeys[key]
	if ok {
		if time.Now().After(expiresAt) {
			delete(s.expiringKeys, key)
			delete(s.data, key)
			s.mu.Unlock()
			return nil, ErrNotFound 
		}
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
	delete(s.data, key)
	return nil
}
