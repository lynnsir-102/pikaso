package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	pk "github.com/lynnsir-102/pikaso"
	pc "github.com/lynnsir-102/pikaso/config"
	w "github.com/lynnsir-102/pikaso/worker"
)

var (
	debug      bool
	pikaHost   string
	pikaPort   int
	pikasoMode string
)

const timeFormat = "2006-01-02 15:04:05"

func init() {
	flag.IntVar(&pikaPort, "port", 9222, "pika port")
	flag.StringVar(&pikaHost, "host", "127.0.0.1", "pika host")
	flag.BoolVar(&debug, "debug", false, "use debug mode")
	flag.StringVar(&pikasoMode, "mode", "sync_sharding", "pikaso mode, dump_classic/sync_classic/sync_sharding")
	flag.Parse()
}

func main() {
	var (
		err error
		ins w.Worker
	)

	switch pikasoMode {
	case "dump_classic":
		ins, err = classicdump()
	case "sync_classic":
		ins, err = classicSync()
	case "sync_sharding":
		ins, err = shardingSync()
	default:
		log.Fatal("pikaso not support this mode\n")
	}

	if err != nil {
		log.Fatalf("pikaso initialize err [%s]\n", err.Error())
	}

	log.Printf("pikaso run as [%s] mode\n", pikasoMode)

	ins.RegisterProcessor(func(row []string) {
		fmt.Printf("%s, receive cmd %v\n", time.Now().Format(timeFormat), row)
	})

	err = ins.Run()
	if err != nil {
		log.Fatalf("pikaso exit, err [%s]\n", err.Error())
	}
}

func classicdump() (w.Worker, error) {
	return pk.NewClassicDumper(&pc.Config{
		Dump: &pc.DumpConfig{
			Debug:    debug,
			PikaHost: pikaHost,
			PikaPort: pikaPort,
		},
	})

}

func classicSync() (w.Worker, error) {
	return pk.NewClassicSyncer(&pc.Config{
		ClassicSync: &pc.ClassicConfig{
			Debug:        debug,
			PikaHost:     pikaHost,
			PikaPort:     pikaPort,
			BinlogFile:   0,
			BinlogOffset: 0,
		},
	})
}

func shardingSync() (w.Worker, error) {
	return pk.NewShardingSyncer(&pc.Config{
		ShardingSync: &pc.ShardingConfig{
			Debug:    debug,
			PikaHost: pikaHost,
			PikaPort: pikaPort,
			Slots: []*pc.SlotConfig{
				&pc.SlotConfig{
					DbName:       "db0",
					PartitionId:  0,
					BinlogFile:   0,
					BinlogOffset: 0,
				},
				&pc.SlotConfig{
					DbName:       "db0",
					PartitionId:  1,
					BinlogFile:   0,
					BinlogOffset: 0,
				},
				&pc.SlotConfig{
					DbName:       "db0",
					PartitionId:  2,
					BinlogFile:   0,
					BinlogOffset: 0,
				},
				&pc.SlotConfig{
					DbName:       "db0",
					PartitionId:  3,
					BinlogFile:   0,
					BinlogOffset: 0,
				},
			},
		},
	})
}
