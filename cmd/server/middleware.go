package main

import (
	"net"

	"github.com/myselfBZ/go-redis-clone/internal/observabilty"
	"github.com/myselfBZ/go-redis-clone/internal/resp"
	"github.com/prometheus/client_golang/prometheus"
)

type metricsMiddeleWareArgs struct {
	Command resp.CommandType
	Conn net.Conn
	Args []resp.RespType
	Handler Handler
}

func metricsMiddleWare(args metricsMiddeleWareArgs) (error) {
	timer := prometheus.NewTimer(observabilty.CommandDuration.WithLabelValues(string(args.Command)))
    defer func() { 
		timer.ObserveDuration() 
		observabilty.CommandsProcessed.WithLabelValues(string(args.Command)).Inc()
	}()

	return args.Handler(args.Conn, args.Args)
}
