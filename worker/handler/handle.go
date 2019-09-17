package handler

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	pr "github.com/lynnsir-102/pikaso/pikaproto"
)

type Handle struct {
	*parser
	*request
	*protocol

	debug     bool
	closed    int32
	echan     chan error
	exchan    chan struct{}
	exitfn    func() error
	firefn    func() error
	commandfn func(cmd []string)

	transport   *transport
	metamanager *metaManager
}

const TimeFormat = "2006-01-02 15:04:05"

var ErrTransportTypeInvalid = errors.New("transport pb response type unknow")

func NewHandle(addr string, fFn, eFn func() error, cFn func(row []string), echan chan error) (*Handle, error) {
	transport, err := newTransport(addr, 3*time.Second)
	if err != nil {
		return nil, err
	}

	handle := &Handle{
		exitfn:      eFn,
		firefn:      fFn,
		commandfn:   cFn,
		transport:   transport,
		metamanager: newMetaManager(),
		echan:       echan,
		exchan:      make(chan struct{}, 1),
	}

	return handle, nil
}

func (h *Handle) Start() error {
	go func() {
		for {
			if !h.isRunning() {
				break
			}

			resp, err := h.transport.read()
			if err != nil {
				h.pushError(err)
				continue
			}

			ir, err := h.decode(resp)
			if err != nil {
				h.pushError(err)
				continue
			}

			err = h.dispatch(ir)
			if err != nil {
				h.pushError(err)
			}
		}
	}()

	return h.firefn()
}

func (h *Handle) ListenEngine() {
	select {
	case <-h.exchan:
		goto CLOSURE
	}

CLOSURE:
	if h.debug {
		fmt.Println("closure handle")
	}
}

func (h *Handle) Stop() error {
	if h.exitfn != nil {
		h.exitfn()
	}

	atomic.StoreInt32(&h.closed, 1)
	h.transport.close()
	close(h.echan)
	h.exchan <- struct{}{}

	return nil
}

func (h *Handle) WithDebug(d bool) {
	h.debug = d
}

func (h *Handle) send(req *pr.InnerRequest) error {
	bytes, err := h.encode(req)
	if err != nil {
		return err
	}

	packet, err := h.buildPacket(bytes)
	if err != nil {
		return err
	}

	if h.debug {
		fmt.Printf("%s, request: %v\n",
			time.Now().Format(TimeFormat), req)
	}

	_, err = h.transport.write(packet)

	return err
}

func (h *Handle) dispatch(ir *pr.InnerResponse) error {

	if h.debug {
		fmt.Printf("%s, response: %v\n",
			time.Now().Format(TimeFormat), ir)
	}

	if ir.GetCode() != pr.StatusCode_kOk {
		return fmt.Errorf("type %s, code %s, reply %s",
			ir.GetType(), ir.GetCode(), ir.GetReply())
	}

	switch ir.GetType() {
	case pr.Type_kMetaSync:
		return h.metaSyncResponse(ir)
	case pr.Type_kTrySync:
		return h.trySyncResponse(ir)
	case pr.Type_kDBSync:
		return h.dbSyncResponse(ir)
	case pr.Type_kBinlogSync:
		return h.binlogResponse(ir)
	case pr.Type_kRemoveSlaveNode:
		return h.removeSlaveNodeResponse(ir)
	default:
		return ErrTransportTypeInvalid
	}
}

func (h *Handle) pushError(err error) {
	if h.isRunning() {
		h.echan <- err
	}
}

func (h *Handle) isRunning() bool {
	return atomic.LoadInt32(&h.closed) == 0
}
