package handler

type metaManager struct {
	metas map[uint32]*binlogMeta
}

func newMetaManager() *metaManager {
	return &metaManager{metas: make(map[uint32]*binlogMeta)}
}

func (m *metaManager) get(i uint32) *binlogMeta {
	if v, ok := m.metas[i]; ok {
		return v
	}

	return nil
}

func (m *metaManager) set(i uint32, params ...interface{}) {
	if v, ok := m.metas[i]; ok {
		v.set(params...)
	} else {
		m.metas[i] = newMeta(params...)
	}
}

func (m *metaManager) getShifts() []map[string]interface{} {
	shifts := make([]map[string]interface{}, 0, len(m.metas))

	for i, v := range m.metas {
		shifts = append(shifts, map[string]interface{}{
			"dbname":       v.db,
			"partitionId":  i,
			"binlogFile":   v.file,
			"binlogOffset": v.offset,
		})
	}

	return shifts
}

type binlogMeta struct {
	db         string
	file       uint32
	offset     uint64
	initFile   *uint32
	initOffset *uint64
}

func newMeta(params ...interface{}) *binlogMeta {
	m := new(binlogMeta)
	m.set(params...)
	return m
}

func (b *binlogMeta) set(params ...interface{}) {
	for _, param := range params {
		switch v := param.(type) {
		case string:
			b.db = v
		case uint32:
			b.file = v
		case uint64:
			b.offset = v
		case *uint32:
			b.initFile = v
		case *uint64:
			b.initOffset = v
		}
	}
}
