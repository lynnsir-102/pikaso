package worker

import (
	"fmt"

	pc "github.com/lynnsir-102/pikaso/config"
	"github.com/lynnsir-102/pikaso/worker/handler"
)

type ClassicSyncer struct {
	h *handler.Handle
	c *pc.ClassicConfig

	echan chan error
}

func NewClassicSyncer(c *pc.Config) (Worker, error) {
	if c.ClassicSync == nil {
		return nil, pc.ErrConfigEmpty
	}
	return &ClassicSyncer{c: c.ClassicSync, echan: make(chan error)}, nil
}

func (s *ClassicSyncer) Run() error {
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

func (s *ClassicSyncer) Stop() error {
	return s.h.Stop()
}

func (s *ClassicSyncer) Errors() chan error {
	return s.echan
}

func (s *ClassicSyncer) fireFn() func() error {
	return func() error {
		s.h.SetMetadata(0, &s.c.BeginFile, &s.c.BeginOffset)

		return s.h.SendMetaSync()
	}
}

func (s *ClassicSyncer) exitFn() func() error {
	return func() error {
		return s.h.SendRemove(0, "db0")
	}
}
