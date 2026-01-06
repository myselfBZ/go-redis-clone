package resp

import(
	"io"
	"fmt"
	"errors"
	"strconv"
)

func WriteError(conn io.Writer, msg string) error {
	msg = fmt.Sprintf("-ERR %s\r\n", msg)
	_, err := conn.Write([]byte(msg))
	return err
} 

func WriteBulkStr(conn io.Writer, s []byte) error {
    if _, err := conn.Write([]byte("$")); err != nil {
        return err
    }
    if _, err := conn.Write([]byte(strconv.Itoa(len(s)))); err != nil {
        return err
    }
    if _, err := conn.Write([]byte("\r\n")); err != nil {
        return err
    }

    if _, err := conn.Write(s); err != nil {
        return err
    }

    if _, err := conn.Write([]byte("\r\n")); err != nil {
        return err
    }

    return nil
}


func WriteNil(conn io.Writer) error {
	_, err := conn.Write([]byte("$-1\r\n"))
	return err
}

func WriteOK(conn io.Writer) error {
	_, err := conn.Write([]byte("+OK\r\n"))
	return err
}

func WritePong(conn io.Writer) error {
	_, err := conn.Write([]byte("+PONG\r\n"))
	return err
}

func WriteSimpleStr(conn io.Writer, data []byte) error {
	if _, err := conn.Write([]byte("+")); err != nil {
		return err
	}

	if _, err := conn.Write(data); err != nil {
		return err
	}

	if _, err := conn.Write([]byte("\r\n")); err != nil {
		return err
	}

	return nil
}

func WriteRespType(conn io.Writer, val RespType) error {
	switch s := val.(type) {
	case *BulkStr:
		return WriteBulkStr(conn, s.Data)
	case *Intiger:
		return WriteInt(conn, s.Data)
	case *SimpleStr:
		return WriteSimpleStr(conn, s.Data)
	case *RespErr:
		return WriteError(conn, string(s.Data))
	case *Nil:
		return WriteNil(conn)
	default:
		return errors.ErrUnsupported
	}
}

func WriteInt(conn io.Writer, i int) error {
	str := fmt.Sprintf(":%d\r\n", i)
	_, err := conn.Write([]byte(str))
	return err
}

