package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	pikaAddr string
	conn     net.Conn
)

func init() {
	flag.StringVar(&pikaAddr, "addr", "127.0.0.1:9222", "pika addr")
	flag.Parse()
}

func init() {
	var err error
	conn, err = net.DialTimeout("tcp", pikaAddr, 1*time.Second)
	if err != nil {
		log.Fatalf("dial err %s", err)
	}
}

func main() {
	body, err := slotInfo()
	if err != nil {
		log.Fatalln(err)
	}

	slots, err := parseBody(body)
	if err != nil {
		log.Fatalln(err)
	}

	byts, err := json.Marshal(slots)
	if err != nil {
		log.Fatalln(err)
	}

	conn.Close()

	fmt.Println(string(byts))
}

func slotInfo() ([]byte, error) {
	cmd := []byte{42, 51, 13, 10, 36, 57, 13, 10, 112, 107, 99, 108, 117, 115, 116, 101, 114, 13, 10, 36, 52, 13, 10, 105, 110, 102, 111, 13, 10, 36, 52, 13, 10, 115, 108, 111, 116, 13, 10}

	_, err := conn.Write(cmd)
	if err != nil {
		return nil, err
	}

	headbuf := make([]byte, 32)
	_, err = conn.Read(headbuf)
	if err != nil {
		return nil, err
	}

	idx := bytes.Index(headbuf, []byte("\r\n"))
	if idx == -1 {
		return nil, errors.New("read headbuf invalid")
	}

	head := headbuf[:idx]
	msgBody := headbuf[idx:]
	headstr := string(head)
	headstr = strings.TrimRight(headstr, "\r\n")
	ls := strings.Replace(headstr, "$", "", -1)
	lenth, err := strconv.Atoi(ls)
	if err != nil {
		return nil, err
	}

	bodybuf := make([]byte, lenth-len(msgBody))
	_, err = io.ReadFull(conn, bodybuf)
	if err != nil {
		return nil, err
	}

	msgBody = append(msgBody, bodybuf...)

	return msgBody, nil
}

func parseBody(body []byte) ([]*slot, error) {
	bs := strings.Split(string(body), "\r\n\r\n")

	var slots []*slot
	for _, v := range bs {
		s, err := parseSlot(v)
		if err != nil {
			return nil, err
		}

		slots = append(slots, s)
	}

	return slots, nil
}

var (
	reg = regexp.MustCompile(`\((.+):(\d+)\)\sbinlog_offset=(\d+)\s(\d+).*`)
)

type slot struct {
	DbName       string `json:"dbname"`
	PartitionId  int    `json:"partitionId"`
	BinlogFile   int    `json:"binlogFile"`
	BinlogOffset int    `json:"binlogOffset"`
}

func parseSlot(str string) (*slot, error) {
	vals := reg.FindStringSubmatch(str)

	if len(vals) != 5 {
		return nil, errors.New("parse slot info invalid")
	}

	index, err := strconv.Atoi(vals[2])
	if err != nil {
		return nil, err
	}

	bFile, err := strconv.Atoi(vals[3])
	if err != nil {
		return nil, err
	}

	bOffset, err := strconv.Atoi(vals[4])
	if err != nil {
		return nil, err
	}

	return &slot{vals[1], index, bFile, bOffset}, nil
}
