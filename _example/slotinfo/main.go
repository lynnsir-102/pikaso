package main

import (
	"encoding/json"
	"flag"
	"log"

	"github.com/lynnsir-102/pikaso/tools/slot"
)

var (
	pikaAddr string
)

func init() {
	flag.StringVar(&pikaAddr, "addr", "127.0.0.1:9222", "pika addr")
	flag.Parse()
}

func main() {
	slots, err := slot.GetSlotInfo(pikaAddr)
	if err != nil {
		log.Fatalln(err)
	}

	byts, err := json.Marshal(slots)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println(string(byts))
}
