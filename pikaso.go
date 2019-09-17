package pikaso

import (
	pc "github.com/lynnsir-102/pikaso/config"
	w "github.com/lynnsir-102/pikaso/worker"
)

func NewClassicDumper(c *pc.Config) (w.Worker, error) {
	return w.NewClassicDumper(c)
}

func NewClassicSyncer(c *pc.Config) (w.Worker, error) {
	return w.NewClassicSyncer(c)
}

func NewShardingSyncer(c *pc.Config) (w.Worker, error) {
	return w.NewShardingSyncer(c)
}
