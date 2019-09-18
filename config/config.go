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
}

type ClassicConfig struct {
	Debug        bool
	PikaHost     string
	PikaPort     int
	BinlogFile   uint32
	BinlogOffset uint64
}

type ShardingConfig struct {
	Debug    bool
	PikaHost string
	PikaPort int
	Slots    []*SlotConfig
}

type SlotConfig struct {
	PartitionId  uint32
	DbName       string
	BinlogFile   uint32
	BinlogOffset uint64
}
