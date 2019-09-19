package handler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const PacketAppendLenth = 4

var (
	ErrConnClosed       = errors.New("connection has be closed")
	ErrLocalAddrInvalid = errors.New("connection local addr invalid")
)

type transport struct {
	ip      string
	port    int32
	conn    net.Conn
	closed  int32
	headbuf []byte
	recvbuf []byte
}

func newTransport(dsn string, timeout time.Duration) (*transport, error) {
	c, err := net.DialTimeout("tcp", dsn, timeout)
	if err != nil {
		return nil, err
	}

	ls := strings.Split(c.LocalAddr().String(), ":")
	if len(ls) != 2 {
		return nil, ErrLocalAddrInvalid
	}

	port, err := strconv.ParseInt(ls[1], 10, 32)
	if err != nil {
		return nil, err
	}

	return &transport{
		ip:      ls[0],
		port:    int32(port),
		conn:    c,
		headbuf: make([]byte, PacketAppendLenth),
	}, nil
}

func (t *transport) read() ([]byte, error) {
	if !t.isValid() {
		return nil, ErrConnClosed
	}

	_, err := t.conn.Read(t.headbuf)
	if err != nil {
		return nil, fmt.Errorf("read headbuf err %s", err.Error())
	}

	mLen := binary.BigEndian.Uint32(t.headbuf)

	if len(t.recvbuf) < int(mLen) {
		b := make([]byte, int(mLen)-len(t.recvbuf))
		t.recvbuf = append(t.recvbuf, b...)
	}

	t.recvbuf = t.recvbuf[:mLen]

	_, err = io.ReadFull(t.conn, t.recvbuf)
	if err != nil {
		return nil, fmt.Errorf("read recvbuf err %s", err.Error())
	}

	return t.recvbuf, nil
}

func (t *transport) write(body []byte) (int, error) {
	if !t.isValid() {
		return 0, ErrConnClosed
	}

	return t.conn.Write(body)
}

func (t *transport) close() error {
	atomic.StoreInt32(&t.closed, 1)

	if t.conn != nil {
		return t.conn.Close()
	}

	return nil
}

func (t *transport) isValid() bool {
	return atomic.LoadInt32(&t.closed) == 0
}
