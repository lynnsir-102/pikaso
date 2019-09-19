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
	debug      bool
	needStop   bool
)

const timeFormat = "2006-01-02 15:04:05"

func init() {
	flag.IntVar(&pikaPort, "port", 9222, "pika port")
	flag.StringVar(&pikaHost, "host", "127.0.0.1", "pika host")
	flag.BoolVar(&debug, "debug", false, "use debug mode")
	flag.BoolVar(&needStop, "stop", false, "stop pikaso after a while")
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
		log.Fatal("💡  not support pikaso mode")
	}

	if err != nil {
		log.Fatalf("💡  initialize err [%s]\n", err.Error())
	}

	log.Printf("🌟  Pikaso run as [%s] mode\n", pikasoMode)

	// the example to stop pikaso
	if needStop {
		go func() {
			time.Sleep(30 * time.Second)
			ins.Stop()
		}()
	}

	// the example to receive pikaso error
	go func() {
		for err := range ins.Errors() {
			log.Printf("pikaso err %s\n", err.Error())
		}
	}()

	// the example to receive pikaso metasoffset
	go func() {
		for {
			time.Sleep(3 * time.Second)
			log.Printf("pikaso offset %v\n", ins.GetMetasOffset())
		}
	}()

	err = ins.Run()
	if err != nil {
		log.Fatalf("💡  Pikaso exit, err [%s]\n", err.Error())
	}
}

func classicdump() (w.Worker, error) {
	w, err := pk.NewClassicDumper(&pc.Config{
		Dump: &pc.DumpConfig{
			Debug:    debug,
			PikaHost: pikaHost,
			PikaPort: pikaPort,
		},
	})

	if err != nil {
		return nil, err
	}

	w.RegisterProcessor(func(row []string) {
		fmt.Printf("in classic dump, %s, receive cmd %v\n", time.Now().Format(timeFormat), row)
	})

	return w, nil
}

func classicSync() (w.Worker, error) {
	w, err := pk.NewClassicSyncer(&pc.Config{
		ClassicSync: &pc.ClassicConfig{
			Debug:        debug,
			PikaHost:     pikaHost,
			PikaPort:     pikaPort,
			BinlogFile:   0,
			BinlogOffset: 0,
		},
	})

	if err != nil {
		return nil, err
	}

	w.RegisterProcessor(func(row []string) {
		fmt.Printf("in classic sync, %s, receive cmd %v\n", time.Now().Format(timeFormat), row)
	})

	return w, nil

}

func shardingSync() (w.Worker, error) {
	w, err := pk.NewShardingSyncer(&pc.Config{
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

	if err != nil {
		return nil, err
	}

	w.RegisterProcessor(func(row []string) {
		fmt.Printf("in sharding sync, %s, receive cmd %v\n", time.Now().Format(timeFormat), row)
	})

	return w, nil

}
