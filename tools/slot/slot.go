package slot

import (
	"bytes"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	conn net.Conn

	command = []byte{42, 51, 13, 10, 36, 57, 13, 10, 112, 107, 99, 108, 117, 115, 116, 101, 114, 13, 10, 36, 52, 13, 10, 105, 110, 102, 111, 13, 10, 36, 52, 13, 10, 115, 108, 111, 116, 13, 10}

	separator      = "\r\n"
	heavyseparator = "\r\n\r\n"
	regularexpr    = regexp.MustCompile(`\((.+):(\d+)\)\sbinlog_offset=(\d+)\s(\d+).*`)
)

func GetSlotInfo(addr string) ([]*slotinfo, error) {
	err := initialize(addr)
	if err != nil {
		return nil, err
	}

	defer destroy()

	packets, err := readPackets()
	if err != nil {
		return nil, err
	}

	return parsePackets(packets)
}

func initialize(addr string) error {
	var err error
	conn, err = net.DialTimeout("tcp", addr, 1*time.Second)
	return err
}

func readPackets() ([]byte, error) {
	_, err := conn.Write(command)
	if err != nil {
		return nil, err
	}

	headbuf := make([]byte, 32)
	_, err = conn.Read(headbuf)
	if err != nil {
		return nil, err
	}

	idx := bytes.Index(headbuf, []byte(separator))
	if idx == -1 {
		return nil, errors.New("read headbuf is unavailable")
	}

	header := strings.Replace(strings.TrimRight(string(headbuf[:idx]), separator), "$", "", -1)
	headerLen, err := strconv.Atoi(header)
	if err != nil {
		return nil, err
	}

	soapbody := headbuf[idx:]
	bodybuf := make([]byte, headerLen-len(soapbody))
	_, err = io.ReadFull(conn, bodybuf)
	if err != nil {
		return nil, err
	}

	soapbody = append(soapbody, bodybuf...)
	return soapbody, nil
}

type slotinfo struct {
	DbName       string `json:"dbname"`
	PartitionId  int    `json:"partitionId"`
	BinlogFile   int    `json:"binlogFile"`
	BinlogOffset int    `json:"binlogOffset"`
}

func parsePackets(packets []byte) ([]*slotinfo, error) {
	pks := strings.Split(string(packets), heavyseparator)
	var slots []*slotinfo
	for _, v := range pks {
		slot, err := parsePacket(v)
		if err != nil {
			return nil, err
		}
		slots = append(slots, slot)
	}
	return slots, nil
}

func parsePacket(packet string) (*slotinfo, error) {
	values := regularexpr.FindStringSubmatch(packet)
	if len(values) != 5 {
		return nil, errors.New("parse slot packet unavailability")
	}

	partitionId, err := strconv.Atoi(values[2])
	if err != nil {
		return nil, err
	}

	binlogFile, err := strconv.Atoi(values[3])
	if err != nil {
		return nil, err
	}

	binlogOffset, err := strconv.Atoi(values[4])
	if err != nil {
		return nil, err
	}

	return &slotinfo{values[1], partitionId, binlogFile, binlogOffset}, nil
}

func destroy() {
	conn.Close()
}
