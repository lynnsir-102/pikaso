package worker

import (
	pc "github.com/lynnsir-102/pikaso/config"
	"github.com/lynnsir-102/pikaso/worker/handler"
)

type ClassicDumper struct {
	h *handler.Handle
	c *pc.DumpConfig
}

func NewClassicDumper(c *pc.Config) (Worker, error) {
	if c.Dump == nil {
		return nil, pc.ErrConfigEmpty
	}
	return &ClassicDumper{c: c.Dump}, nil
}

func (d *ClassicDumper) Run() error {
	return nil
}

func (s *ClassicDumper) Stop() error {
	return nil
}

func (d *ClassicDumper) fireFn() func() error {
	return func() error {
		return d.h.SendDbSync(0, 0, 0, "db0")
	}
}
