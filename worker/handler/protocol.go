package handler

import (
	"bytes"
	"encoding/binary"

	proto "github.com/golang/protobuf/proto"

	"github.com/lynnsir-102/pikaso/pikaproto"
)

type protocol struct{}

func (p *protocol) encode(request *pikaproto.InnerRequest) ([]byte, error) {
	return proto.Marshal(request)
}

func (p *protocol) decode(response []byte) (*pikaproto.InnerResponse, error) {
	ip := &pikaproto.InnerResponse{}
	err := proto.Unmarshal(response, ip)
	return ip, err
}

func (p *protocol) buildPacket(body []byte) ([]byte, error) {
	buf, err := p.appendToBytes(len(body))
	if err != nil {
		return nil, err
	}

	buf = append(buf, body...)
	return buf, nil
}

func (p *protocol) appendToBytes(n int) ([]byte, error) {
	t := int32(n)
	buf := bytes.NewBuffer([]byte{})

	err := binary.Write(buf, binary.BigEndian, &t)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
