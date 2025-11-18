package resp

import(
	"io"
	"fmt"
	"errors"
)

func WriteError(conn io.Writer, msg string) error {
	msg = fmt.Sprintf("-ERR %s\r\n", msg)
	_, err := conn.Write([]byte(msg))
	return err
} 

func WriteBulkStr(conn io.Writer, s string) error {
	msg := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	_, err := conn.Write([]byte(msg))
	return err
}


func WriteNil(conn io.Writer) error {
	_, err := conn.Write([]byte("$-1\r\n"))
	return err
}

func WriteOK(conn io.Writer) error {
	_, err := conn.Write([]byte("+OK\r\n"))
	return err
}

func WriteRespType(conn io.Writer, val RespType) error {
	switch s := val.(type) {
	case *BulkStr:
		return WriteBulkStr(conn, s.Data)
	default:
		return errors.ErrUnsupported
	}
}

