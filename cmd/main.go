package main

import (
	"io"
	"log/slog"
	"net"

	"github.com/myselfBZ/redis-clone/internal/resp"
	"github.com/myselfBZ/redis-clone/internal/store"
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

		slog.Info("recieved a new command", "data", command.ArgsString())
		args := command.Args()

		// if len(args) <= 2 {
		// 	return resp.WriteBulkStr(conn, "delayed response")
		// }

		switch c := args[0].(type) {

		case *resp.BulkStr:

			if resp.SET == resp.CommandType(c.Data) {
				slog.Info("They are equal")
			}

			handler, ok := commandHandlers[resp.CommandType(c.Data)]
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
			resp.WriteBulkStr(conn, "invalid data type")
		}
	}
}

func main() {
	storage := store.NewStorage()
	server := newServer(storage)

	commandHandlers[resp.CommandType("SET")] = server.handleSet
	commandHandlers[resp.CommandType("GET")] = server.handleGet
	commandHandlers[resp.COMMAND_DOCS] = server.handleCommandDocs

	slog.Info("server started...")
	if err := server.run(); err != nil {
		slog.Error("server stopped", "error", err.Error())
	}
}
