package store

import (
	"errors"
	"sync"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
)


var (
	ErrNotFound = errors.New("key not found")
)

func NewStorage() *Storage {
	return &Storage{
		mu: &sync.RWMutex{},
		data: make(map[string]resp.RespType),
	}
}

type Storage struct {
	mu *sync.RWMutex
	data map[string]resp.RespType
}

func (s *Storage) Set(key string, val resp.RespType)  {
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}

func (s *Storage) Get(key string) (resp.RespType ,error) {
	s.mu.RLock()
	data, ok := s.data[key]
	s.mu.RUnlock()

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
