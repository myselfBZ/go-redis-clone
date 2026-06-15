package main

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"net/http"
	"runtime"

	"github.com/myselfBZ/go-redis-clone/internal/observabilty"
	"github.com/myselfBZ/go-redis-clone/internal/resp"
	"github.com/myselfBZ/go-redis-clone/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type server struct {
	storage *store.Storage
	ln      net.Listener
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

			slog.Error("failed to accept connection s.ln.Accept()", "error", err.Error())
			continue
		}

		slog.Info("connection accepted")
		go s.handle(conn)
	}
}

func (s *server) run() error {
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		return err
	}

	s.ln = ln
	go s.storage.StartJanitor()
	go func ()  {
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
            observabilty.MemoryUsage.Set(float64(m.Alloc))
			time.Sleep(time.Millisecond * 500)
		}
	}()
	return s.accept()
}

func newServer(storage *store.Storage) *server {
	return &server{
		storage: storage,
	}
}

func (s *server) handle(conn net.Conn)  {
	defer func() { 
		conn.Close() 
		observabilty.ActiveConnections.Dec()
	}()
	observabilty.ActiveConnections.Inc()
	for {
		command, err := resp.Parse(conn)

		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				slog.Info("Client disconnected")
				return
			}

			// protocol error
			respErr := resp.RespErr{
				Data: []byte(err.Error()),
			}

			if _, err := conn.Write(respErr.ToBytes()); err != nil {
				slog.Info("Client disconnected")
				return
			}
			continue
		}

		args := command.Args()
		switch c := args[0].(type) {

		case *resp.BulkStr:

			handler, ok := commandHandlers[resp.CommandType(strings.ToUpper(c.String()))]
			if !ok {
				respErr := resp.RespErr{
					Data: []byte("invalid command"),
				}
				conn.Write(respErr.ToBytes())
				continue
		}

			res := metricsMiddleWare(metricsMiddeleWareArgs{
				Conn: conn,
				Args: args,
				Handler: handler,
			})

			if _, err := conn.Write(res.Data.ToBytes()); err != nil {
				slog.Error("connection write error", "err", err)
				return
			}

		default:
			respErr := resp.RespErr{
				Data: []byte("invalid protocol"),
			}
			if _, err := conn.Write(respErr.ToBytes()); err != nil {
				slog.Error("connection write error", "err", err)
				return
			}
		}
	}
}

func main() {
	storage := store.NewStorage()
	server := newServer(storage)

	commandHandlers[resp.SET] = server.handleSet
	commandHandlers[resp.GET] = server.handleGet
	commandHandlers[resp.DEL] = server.handleDel
	commandHandlers[resp.COMMAND_DOCS] = server.handleCommandDocs
	commandHandlers[resp.TTL] = server.handleTTL
	commandHandlers[resp.EXPIRE] = server.handleExpire
	commandHandlers[resp.PTTL] = server.handlePTTL
	commandHandlers[resp.PERSIST] = server.handlePersist
	commandHandlers[resp.INCR] = server.handleIncr
	commandHandlers[resp.DECR] = server.handleDecr
	commandHandlers[resp.INCRBY] = server.handleIncrBy
	commandHandlers[resp.PING] = server.handlePing

	// prometheus
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":2112", nil)

	slog.Info("server started...")
	if err := server.run(); err != nil {
		slog.Error("server stopped", "error", err.Error())
	}
}
