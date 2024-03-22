package toyboltdb

const (
	magic   = uint32(0xED0CDAED) // QQQ: deadcode?
	version = 1
)

type meta struct {
	magic          uint32
	version        uint32
	pageSize       uint32
	flags          uint32
	bucketsPageID  pageID
	freelistPageID pageID
	pageID         pageID
	txID           txID
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	}
	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	dest.magic = m.magic
	dest.version = m.version
	dest.pageSize = m.pageSize
	dest.bucketsPageID = m.bucketsPageID
	dest.freelistPageID = m.freelistPageID
	dest.pageID = m.pageID
	dest.txID = m.txID

	// NOTE: This is NG
	// dest = &meta{
	// 	magic:          m.magic,
	// 	version:        m.version,
	// 	pageSize:       m.pageSize,
	// 	flags:          m.flags,
	// 	bucketsPageID:  m.bucketsPageID,
	// 	freelistPageID: m.freelistPageID,
	// 	pageID:         m.pageID,
	// 	txID:           m.txID,
	// }
}

// write writes the meta onto a page.
func (m *meta) write(p *page) {
	// Page id is either going to be 0 or 1 which we can determine by the Txn ID.
	p.id = pageID(m.txID % 2)
	p.flags |= metaPageFlag

	m.copy(p.meta())
}
