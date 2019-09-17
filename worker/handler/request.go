package handler

import (
	pr "github.com/lynnsir-102/pikaso/pikaproto"
)

type request struct{}

func (r *request) metaSync(
	ip string, port int32) *pr.InnerRequest {

	return &pr.InnerRequest{
		Type: pr.Type_kMetaSync.Enum(),
		MetaSync: &pr.InnerRequest_MetaSync{
			Node: &pr.Node{
				Ip:   &ip,
				Port: &port,
			},
		},
	}
}

func (r *request) partitionTrySync(
	ip string, port int32,
	file uint32, offset uint64,
	table string, pid uint32) *pr.InnerRequest {

	return &pr.InnerRequest{
		Type: pr.Type_kTrySync.Enum(),
		TrySync: &pr.InnerRequest_TrySync{
			Node: &pr.Node{
				Ip:   &ip,
				Port: &port,
			},
			Partition: &pr.Partition{
				TableName:   &table,
				PartitionId: &pid,
			},
			BinlogOffset: &pr.BinlogOffset{
				Filenum: &file,
				Offset:  &offset,
			},
		},
	}
}

func (r *request) partitionDBSync(
	ip string, port int32,
	file uint32, offset uint64,
	table string, pid uint32) *pr.InnerRequest {

	return &pr.InnerRequest{
		Type: pr.Type_kDBSync.Enum(),
		DbSync: &pr.InnerRequest_DBSync{
			Node: &pr.Node{
				Ip:   &ip,
				Port: &port,
			},
			Partition: &pr.Partition{
				TableName:   &table,
				PartitionId: &pid,
			},
			BinlogOffset: &pr.BinlogOffset{
				Filenum: &file,
				Offset:  &offset,
			},
		},
	}
}

func (r *request) partitionBinlogSync(
	ip string, port int32,
	sFile uint32, sOffset uint64,
	eFile uint32, eOffset uint64,
	table string, pid uint32,
	sid int32, isFirst bool) *pr.InnerRequest {

	return &pr.InnerRequest{
		Type: pr.Type_kBinlogSync.Enum(),
		BinlogSync: &pr.InnerRequest_BinlogSync{
			Node: &pr.Node{
				Ip:   &ip,
				Port: &port,
			},
			TableName: &table, PartitionId: &pid,
			SessionId: &sid, FirstSend: &isFirst,
			AckRangeStart: &pr.BinlogOffset{
				Filenum: &sFile,
				Offset:  &sOffset,
			},
			AckRangeEnd: &pr.BinlogOffset{
				Filenum: &eFile,
				Offset:  &eOffset,
			},
		},
	}
}

func (r *request) removeSlaveNode(
	ip string, port int32,
	table string, pid uint32) *pr.InnerRequest {

	return &pr.InnerRequest{
		Type: pr.Type_kRemoveSlaveNode.Enum(),
		RemoveSlaveNode: []*pr.InnerRequest_RemoveSlaveNode{
			&pr.InnerRequest_RemoveSlaveNode{
				Node: &pr.Node{
					Ip:   &ip,
					Port: &port,
				},
				Partition: &pr.Partition{
					TableName:   &table,
					PartitionId: &pid,
				},
			},
		},
	}
}
