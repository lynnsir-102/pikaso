package handler

import (
	"errors"

	"github.com/lynnsir-102/pikaso/redis"
)

type parser struct{}

const BinglogItemHeaderSize = 34

var ErrBinlogItemLenthInvalid = errors.New("binlog item lenth invalid")

func (p *parser) parse(binlog []byte) ([]string, error) {
	if len(binlog) < BinglogItemHeaderSize {
		return nil, ErrBinlogItemLenthInvalid
	}

	raw := binlog[BinglogItemHeaderSize:]

	bulk, err := redis.DecodeFromBytes(raw)
	if err != nil {
		return nil, err
	}

	cmd := make([]string, 0, len(bulk.Array))

	for _, item := range bulk.Array {
		if item.Type == redis.TypeBulkBytes {
			cmd = append(cmd, string(item.Value))
		}
	}

	return cmd, nil
}
