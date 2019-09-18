package worker

import (
	"fmt"

	pc "github.com/lynnsir-102/pikaso/config"
	"github.com/lynnsir-102/pikaso/worker/handler"
)

type ShardingSyncer struct {
	h *handler.Handle
	c *pc.ShardingConfig
}

func NewShardingSyncer(c *pc.Config) (Worker, error) {
	if c.ShardingSync == nil {
		return nil, pc.ErrConfigEmpty
	}

	s := &ShardingSyncer{c: c.ShardingSync}

	var err error
	addr := fmt.Sprintf("%s:%d", s.c.PikaHost, s.c.PikaPort+2000)
	s.h, err = handler.NewHandle(addr, s.fireFn(), s.exitFn(), s.c.RowFunc)
	if err != nil {
		return nil, err
	}

	s.h.WithDebug(s.c.Debug)

	return s, nil
}

func (s *ShardingSyncer) Run() error {
	err := s.h.Start()
	if err != nil {
		return err
	}

	s.h.ListenEngine()

	return nil
}

func (s *ShardingSyncer) Stop() error {
	return s.h.Stop()
}

func (s *ShardingSyncer) Errors() chan error {
	return s.h.GetErrors()
}

func (s *ShardingSyncer) GetMetasOffset() []map[string]interface{} {
	return s.h.GetMetasOffset()
}

func (s *ShardingSyncer) fireFn() func() error {
	return func() error {
		var err error

		for _, v := range s.c.Slots {
			s.h.SetMetadata(v.PartitionId, &v.BinlogFile, &v.BinlogOffset, v.DbName)
		}

		for _, v := range s.c.Slots {
			err = s.h.SendTrySync(v.PartitionId, v.BinlogFile, v.BinlogOffset, v.DbName)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func (s *ShardingSyncer) exitFn() func() error {
	return func() error {
		var err error

		for _, v := range s.c.Slots {
			err = s.h.SendRemove(v.PartitionId, v.DbName)
			if err != nil {
				return err
			}
		}

		return nil
	}
}
