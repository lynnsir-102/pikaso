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

type binlogMeta struct {
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
