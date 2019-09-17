package handler

import (
	"errors"
	"fmt"
	"time"

	pr "github.com/lynnsir-102/pikaso/pikaproto"
)

var (
	ErrMetaSyncTableEmpty = errors.New("metaSync response table empty")
)

func (h *Handle) metaSyncResponse(resp *pr.InnerResponse) error {
	msync := resp.GetMetaSync()
	tables := msync.GetTablesInfo()

	if len(tables) == 0 {
		return ErrMetaSyncTableEmpty
	}

	t := tables[0]

	tName := t.GetTableName()
	pid := uint32(t.GetPartitionNum() - 1)

	return h.send(h.partitionTrySync(
		h.transport.ip, h.transport.port, 0, 0, tName, pid))
}

func (h *Handle) trySyncResponse(resp *pr.InnerResponse) error {
	tsync := resp.GetTrySync()
	partition := tsync.GetPartition()
	sid := tsync.GetSessionId()
	pid := partition.GetPartitionId()
	tName := partition.GetTableName()

	var (
		file   uint32
		offset uint64
	)

	if meta := h.metamanager.get(pid); meta != nil {
		if meta.initFile != nil {
			file = *meta.initFile
		}
		if meta.initOffset != nil {
			offset = *meta.initOffset
		}
	}

	return h.send(h.partitionBinlogSync(
		h.transport.ip, h.transport.port,
		file, offset, file, offset, tName, pid, sid, true))
}

func (h *Handle) dbSyncResponse(resp *pr.InnerResponse) error {
	return nil
}

func (h *Handle) binlogResponse(resp *pr.InnerResponse) error {
	bSyncs := resp.GetBinlogSync()

	group := make(map[string][]*pr.InnerResponse_BinlogSync)

	for _, v := range bSyncs {
		part := v.GetPartition()
		key := fmt.Sprintf("%d-%s", part.GetPartitionId(), part.GetTableName())
		group[key] = append(group[key], v)
	}

	for _, bSyncs := range group {

		var (
			sid   int32
			pid   uint32
			tName string

			startFile   uint32
			startOffset uint64

			binOffset *pr.BinlogOffset
		)

		for i, v := range bSyncs {
			binlog := v.GetBinlog()

			if i == 0 {
				sid = v.GetSessionId()
				pid = v.GetPartition().GetPartitionId()
				tName = v.GetPartition().GetTableName()
			}

			if binlog != nil {
				bs := v.GetBinlogOffset()
				if binOffset == nil {
					startFile = bs.GetFilenum()
					startOffset = bs.GetOffset()
				}

				binOffset = bs

				if bs.GetFilenum() == 0 && bs.GetOffset() == 0 {
					fmt.Printf("%s, partition %d, receive master heartbeat ❤️ ❤️ ❤️\n", time.Now().Format(TimeFormat), pid)
					continue
				}

				cmd, err := h.parse(binlog)
				if err != nil {
					return err
				}

				if h.commandfn != nil {
					h.commandfn(cmd)
				}
			}
		}

		if !(binOffset.GetFilenum() == 0 && binOffset.GetOffset() == 0) {
			h.metamanager.set(pid, binOffset.GetFilenum(), binOffset.GetOffset())
		}

		err := h.send(h.partitionBinlogSync(
			h.transport.ip, h.transport.port,
			startFile, startOffset,
			binOffset.GetFilenum(), binOffset.GetOffset(),
			tName, pid, sid, false))
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *Handle) removeSlaveNodeResponse(resp *pr.InnerResponse) error {
	return nil
}
