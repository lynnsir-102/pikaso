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
	pikaHost   string
	pikaPort   int
	pikasoMode string
	needStop   bool
)

func init() {
	flag.IntVar(&pikaPort, "port", 9222, "pika port")
	flag.StringVar(&pikaHost, "host", "127.0.0.1", "pika host")
	flag.BoolVar(&needStop, "stop", false, "stop pikaso after a while")
	flag.StringVar(&pikasoMode, "mode", "sync_classic", "pikaso mode, dump_classic/sync_classic/sync_sharding")
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
		log.Fatal("💡  not support pikaso mode")
	}

	if err != nil {
		log.Fatalf("💡  initialize err [%s]\n", err.Error())
	}

	log.Printf("🌟  Pikaso run as [%s] mode\n", pikasoMode)

	if needStop {
		go func() {
			time.Sleep(30 * time.Second)
			ins.Stop()
		}()
	}

	err = ins.Run()
	if err != nil {
		log.Fatalf("💡  Pikaso exit, err [%s]\n", err.Error())
	}
}

func classicdump() (w.Worker, error) {
	return pk.NewClassicDumper(&pc.Config{
		Dump: &pc.DumpConfig{
			Debug:    false,
			PikaHost: pikaHost,
			PikaPort: pikaPort,
			RowFunc: func(row []string) {
				fmt.Printf("in dump, row func receive cmd, %v\n", row)
			},
		},
	})
}

func classicSync() (w.Worker, error) {
	return pk.NewClassicSyncer(&pc.Config{
		ClassicSync: &pc.ClassicConfig{
			Debug:       false,
			PikaHost:    pikaHost,
			PikaPort:    pikaPort,
			BeginFile:   0,
			BeginOffset: 0,
			RowFunc: func(row []string) {
				fmt.Printf("in classic, row func receive cmd, %v\n", row)
			},
		},
	})
}

func shardingSync() (w.Worker, error) {
	return pk.NewShardingSyncer(&pc.Config{
		ShardingSync: &pc.ShardingConfig{
			Debug:    false,
			PikaHost: pikaHost,
			PikaPort: pikaPort,
			Slots: []*pc.SlotConfig{
				&pc.SlotConfig{
					Index:       0,
					DbName:      "db0",
					BeginFile:   0,
					BeginOffset: 1091,
				},
				&pc.SlotConfig{
					Index:       1,
					DbName:      "db0",
					BeginFile:   0,
					BeginOffset: 0,
				},
				&pc.SlotConfig{
					Index:       2,
					DbName:      "db0",
					BeginFile:   0,
					BeginOffset: 0,
				},
				&pc.SlotConfig{
					Index:       3,
					DbName:      "db0",
					BeginFile:   0,
					BeginOffset: 0,
				},
			},
			RowFunc: func(row []string) {
				fmt.Printf("in sharding, row func receive cmd, %v\n", row)
			},
		},
	})
}
