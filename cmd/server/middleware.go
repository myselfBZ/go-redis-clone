package main
import(
	"net"
    "github.com/prometheus/client_golang/prometheus"
	"github.com/myselfBZ/go-redis-clone/internal/resp"
)

type metricsMiddeleWareArgs struct {
	Command resp.CommandType
	Conn net.Conn
	Args []resp.RespType
	Handler Handler
}

func metricsMiddleWare(args metricsMiddeleWareArgs) (error) {
	timer := prometheus.NewTimer(commandDuration.WithLabelValues(string(args.Command)))
    defer func() { 
		timer.ObserveDuration() 
		commandsProcessed.WithLabelValues(string(args.Command)).Inc()
	}()

	return args.Handler(args.Conn, args.Args)
}
