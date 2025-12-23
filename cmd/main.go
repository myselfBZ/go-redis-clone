package main

import (
	"io"
	"log/slog"
	"net"
	"strings"

	"github.com/myselfBZ/go-redis-clone/internal/resp"
	"github.com/myselfBZ/go-redis-clone/internal/store"
)

type server struct {
	storage *store.Storage
	ln      net.Listener
}

func (s *server) accept() error {
	for {
		conn, err := s.ln.Accept()

		if err != nil {
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
	return s.accept()
}

func newServer(storage *store.Storage) *server {
	return &server{
		storage: storage,
	}
}

func (s *server) handle(conn net.Conn) error {
	defer conn.Close()
	for {

		command, err := resp.CommandFromReader(conn)

		if err != nil {
			if err == io.EOF {
				slog.Info("Client disconnected")
				return nil
			}

			resp.WriteBulkStr(conn, err.Error())
			continue
		}

		args := command.Args()

		// if len(args) <= 2 {
		// 	return resp.WriteBulkStr(conn, "delayed response")
		// }

		switch c := args[0].(type) {

		case *resp.BulkStr:

			handler, ok := commandHandlers[resp.CommandType(strings.ToUpper(c.Data))]
			if !ok {
				resp.WriteError(conn, "invalid command")
				break
			}

			if err := handler(conn, args); err != nil {
				if err == io.EOF {
					return nil
				}

				slog.Error("handler error", "error", err)
			}

		default:
			resp.WriteBulkStr(conn, "invalid protocol")
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

	slog.Info("server started...")
	if err := server.run(); err != nil {
		slog.Error("server stopped", "error", err.Error())
	}
}
