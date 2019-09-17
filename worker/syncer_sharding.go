package worker

import (
	"fmt"

	pc "github.com/lynnsir-102/pikaso/config"
	"github.com/lynnsir-102/pikaso/worker/handler"
)

type ShardingSyncer struct {
	h *handler.Handle
	c *pc.ShardingConfig

	echan chan error
}

func NewShardingSyncer(c *pc.Config) (Worker, error) {
	if c.ShardingSync == nil {
		return nil, pc.ErrConfigEmpty
	}

	return &ShardingSyncer{c: c.ShardingSync, echan: make(chan error)}, nil
}

func (s *ShardingSyncer) Run() error {
	var err error

	addr := fmt.Sprintf("%s:%d", s.c.PikaHost, s.c.PikaPort+2000)
	s.h, err = handler.NewHandle(addr, s.fireFn(), s.exitFn(), s.c.RowFunc, s.echan)
	if err != nil {
		return err
	}

	s.h.WithDebug(s.c.Debug)

	err = s.h.Start()
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
	return s.echan
}

func (s *ShardingSyncer) fireFn() func() error {
	return func() error {
		var err error

		for _, v := range s.c.Slots {
			s.h.SetMetadata(v.Index, &v.BeginFile, &v.BeginOffset, v.DbName)
		}

		for _, v := range s.c.Slots {
			err = s.h.SendTrySync(v.Index, v.BeginFile, v.BeginOffset, v.DbName)
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
			err = s.h.SendRemove(v.Index, v.DbName)
			if err != nil {
				return err
			}
		}

		return nil
	}
}
