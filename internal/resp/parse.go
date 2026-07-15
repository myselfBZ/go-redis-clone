package resp

import (
	"bytes"
	"errors"
	"io"
	"strconv"
)

var (
	ErrInvalidHeaderLength = errors.New("invalid header length")
	ErrHeaderSizeTooBig    = errors.New("header for this content is too big")
	ErrRequireMultiBulk    = errors.New("require multi bulk protocol")
	ErrBulkSizeTooBig      = errors.New("bulk string size exceeds 512 MB")
	ErrFooterMissing       = errors.New("footer is missing")
)

const (
	MAX_HEADER_LENGHT = 20
	MAX_BULK_SIZE     = 512 << 20
)

const (
	payloadStateInit         = "init"
	payloadStateParseContent = "parse content"
	payloadStateDone         = "done"
)

type bulk struct {
	length int64
	data   []byte
}

type payload struct {
	state string

	curIdx    int64
	arrLength int64
	multiBulk []*bulk
}

func resumeParsing(b *bulk, data []byte) (int, bool, error) {
	dataLen := int64(len(data))
	length := b.length - int64(len(b.data))
	if length == 0 {
		if dataLen >= 2 {
			footer := data[:2]
			if !bytes.Equal(footer, []byte(CRLF)) {
				return 0, false, ErrFooterMissing
			}
			return 2, true, nil
		}
		return 0, false, nil
	}
	if length+2 <= dataLen {
		neededData := data[:length]
		footer := data[length : length+2]
		if !bytes.Equal(footer, []byte(CRLF)) {
			return 0, false, ErrFooterMissing
		}
		b.data = append(b.data, neededData...)
		return len(neededData) + 2, true, nil
	}
	if length <= dataLen {
		neededData := data[:length]
		b.data = append(b.data, neededData...)
		return len(neededData), false, nil
	}
	b.data = append(b.data, data...)
	return len(data), false, nil
}

func (p *payload) parseBytes(data []byte) (int, error) {
	read := 0
	for {
		switch p.state {
		case payloadStateInit:
			idx := bytes.Index(data, []byte(CRLF))
			if idx < 0 && len(data) > MAX_HEADER_LENGHT {
				return 0, ErrHeaderSizeTooBig
			}

			if idx < 0 {
				return 0, nil
			}

			h := data[:idx]
			if h[0] != '*' {
				return 0, ErrRequireMultiBulk
			}

			length, err := strconv.ParseInt(string(h[1:idx]), 10, 64)
			if err != nil {
				return 0, ErrInvalidHeaderLength
			}
			p.arrLength = length
			read += len(h) + len(CRLF)
			p.state = payloadStateParseContent
		case payloadStateParseContent:
			for p.curIdx < p.arrLength {
				if int64(len(p.multiBulk)) > p.curIdx {
					b := p.multiBulk[p.curIdx]
					r, complete, err := resumeParsing(b, data)
					if err != nil {
						return 0, err
					}
					if complete {
						p.multiBulk[p.curIdx] = b
						p.curIdx++
						read += r
						continue
					}
					return r, nil
				}
				body := data[read:]
				idx := bytes.Index(body, []byte(CRLF))
				if idx < 0 && len(body) > MAX_HEADER_LENGHT {
					return 0, ErrHeaderSizeTooBig
				}
				if idx < 0 {
					return read, nil
				}
				h := body[:idx]
				if h[0] != '$' {
					return 0, ErrRequireMultiBulk
				}
				length, err := strconv.ParseInt(string(h[1:idx]), 10, 64)
				if err != nil {
					return 0, ErrInvalidHeaderLength
				}

				if length > MAX_BULK_SIZE {
					return 0, ErrBulkSizeTooBig
				}

				b := &bulk{
					length: length,
					data:   make([]byte, 0),
				}
				read += len(h) + 2

				if length+2 <= int64(len(body[idx+2:])) {
					start := idx + 2
					end := idx + 2 + int(length)
					bulkData := make([]byte, length)
					copy(bulkData, body[start:end])
					if !bytes.Equal(body[end:end+2], []byte(CRLF)) {
						return 0, ErrFooterMissing
					}

					b.data = bulkData
					p.multiBulk = append(p.multiBulk, b)
					read += len(bulkData) + len(CRLF)
					p.curIdx++
				} else {
					b.data = append(b.data, body[idx+2:]...)
					read += len(body[idx+2:])
					p.multiBulk = append(p.multiBulk, b)
					return read, nil
				}
			}
			p.state = payloadStateDone
		case payloadStateDone:
			return read, nil
		}
	}
}

func Parse(stream io.Reader) <-chan *Command {
	ch := make(chan *Command)
	go parse(stream, ch)
	return ch
}

func parse(stream io.Reader, ch chan<- *Command) {
	for {
		p := &payload{
			state:     payloadStateInit,
			multiBulk: make([]*bulk, 0),
		}

		buff := make([]byte, 4096)
		buffLen := 0

		for p.state != payloadStateDone {
			n, err := stream.Read(buff[buffLen:])

			if err != nil {
				ch <- &Command{Err: err}
				return
			}

			buffLen += n

			readN, err := p.parseBytes(buff[:buffLen])

			if err != nil {
				ch <- &Command{Err: err}
				return
			}

			copy(buff, buff[readN:buffLen])
			buffLen -= readN
		}

		c := &Command{
			arr: new(RespBulkStrArr),
		}
		for _, b := range p.multiBulk {
			c.arr.Append(b.data)
		}
		ch <- c
	}
}
