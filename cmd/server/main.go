package main

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"


	"github.com/myselfBZ/go-redis-clone/internal/resp"
	"github.com/myselfBZ/go-redis-clone/internal/store"
)

type server struct {
	// net.Conn -> placeholder (struct{}{})
	conns	sync.Map 	
	storage *store.Storage
	ln      net.Listener

	closing atomic.Bool
}

func (s *server) accept() error {
	for {
		conn, err := s.ln.Accept()

		if err != nil {

			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				slog.Info("temporary accept outage: retry in 5ms", "error", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}

			// Listener closed intentionally
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			return err
		}
		go s.handle(conn) 
	}
}

func (s *server) run() error {
	errCh := make(chan error)
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigCh
		close(closeChan)
	}()

	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		return err
	}

	s.ln = ln

	go func () { 
		if err := s.accept(); err != nil {
			errCh <- err
		} 
	}()

	select {
	case err = <- errCh:
	case <- closeChan:
		slog.Info("Graceful shut down intialized...")
	}

	s.close()

	return err
}

func (s *server) close() {
	s.closing.Store(true)
	s.ln.Close()
	s.conns.Range(func(key, value any) bool {
		conn := key.(net.Conn)
		conn.Close()
		return true
	})

	// resetting 
	s.conns = sync.Map{}
	s.storage.Close()
}

func (s *server) closeClient(conn net.Conn) {
	conn.Close()
	s.conns.Delete(conn)
}

func newServer(storage *store.Storage) *server {
	return &server{
		storage: storage,
		conns: sync.Map{},
		closing: atomic.Bool{},
	}
}

func (s *server) handle(conn net.Conn) {
	if s.closing.Load() {
		conn.Close()
		return
	}

	slog.Info("Connection accepted")
	s.conns.Store(conn, struct{}{})

	defer func() {
		slog.Info("Closing client connection")
		s.closeClient(conn)
	}()

	for {
		command, err := resp.Parse(conn)

		if err != nil {
			if errors.Is(err, io.EOF) || 
			errors.Is(err, io.ErrUnexpectedEOF) || 
			strings.Contains(err.Error(), "use of closed network connection") {
				return
			}

			// protocol error
			respErr := resp.RespErr{
				Data: []byte(err.Error()),
			}

			if _, err := conn.Write(respErr.ToBytes()); err != nil {
				return
			}

			continue
		}

		args := command.Args()

		res := s.storage.Exec(args)

		if _, err := conn.Write(res.ToBytes()); err != nil {
			slog.Error("connection write error", "err", err)
			return
		}
	}

}

func main() {
	storage := store.NewStorage()
	server := newServer(storage)

	slog.Info("server started...")
	if err := server.run(); err != nil {
		slog.Error("server stopped", "error", err.Error())
	}

	slog.Info("server stopped without any issues")
}
