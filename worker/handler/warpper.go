package handler

func (h *Handle) SetMetadata(i uint32, params ...interface{}) {
	h.metamanager.set(i, params...)
}

func (h *Handle) SendMetaSync() error {
	return h.send(h.metaSync(h.transport.ip, h.transport.port))
}

func (h *Handle) SendTrySync(pid, file uint32, offset uint64, table string) error {
	return h.send(h.partitionTrySync(h.transport.ip, h.transport.port, file, offset, table, pid))
}

func (h *Handle) SendDbSync(pid, file uint32, offset uint64, table string) error {
	return h.send(h.partitionDBSync(h.transport.ip, h.transport.port, file, offset, table, pid))
}

func (h *Handle) SendRemove(pid uint32, table string) error {
	return h.send(h.removeSlaveNode(h.transport.ip, h.transport.port, table, pid))
}
