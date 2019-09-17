package config

import "errors"

var (
	ErrConfigEmpty = errors.New("pikaso config empty")
)

type Config struct {
	Dump         *DumpConfig
	ClassicSync  *ClassicConfig
	ShardingSync *ShardingConfig
}

type DumpConfig struct {
	Debug    bool
	PikaHost string
	PikaPort int
	RowFunc  func(row []string)
}

type ClassicConfig struct {
	Debug       bool
	PikaHost    string
	PikaPort    int
	BeginFile   uint32
	BeginOffset uint64
	RowFunc     func(row []string)
}

type ShardingConfig struct {
	Debug    bool
	PikaHost string
	PikaPort int
	Slots    []*SlotConfig
	RowFunc  func(row []string)
}

type SlotConfig struct {
	Index       uint32
	DbName      string
	BeginFile   uint32
	BeginOffset uint64
}
