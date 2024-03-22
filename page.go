// For more information on boltdb's page allocation,
// see [this comment](https://github.com/boltdb/bolt/issues/308#issuecomment-74811638).
//
// |M|M|F|D| | | | | |
//
// metadata pages (M),
// free pages (F),
// data (D),
// unallocated ( )
//
// the page size (typically 4KB)
//
// If you want to understand how database pages work generally,
// see [Database Pages — A deep dive](https://medium.com/@hnasr/database-pages-a-deep-dive-38cdb2c79eb5)
package toyboltdb

import (
	"fmt"
	"unsafe"
)

const (
	pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

	branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
	leafPageElementSize   = int(unsafe.Sizeof(leafPageElement{}))
)

const (
	branchPageFlag   = 0x01 // 0b00001
	leafPageFlag     = 0x02 // 0b00010
	metaPageFlag     = 0x04 // 0b00100
	bucketsPageFlag  = 0x08 // 0b01000
	freelistPageFlag = 0x10 // 0b10000
)

const (
	maxNodesPerPage = 65535     // 16bit
	maxAllocSize    = 0xFFFFFFF // 28bit
	minKeysPerPage  = 2
)

type pageID uint64

type page struct {
	id       pageID
	flags    uint16
	count    uint16
	overflow uint32
	ptr      uintptr
}

// pageElementRef represents a reference to an element on a given page.
type pageElementRef struct {
	page  *page
	index uint16
}

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & bucketsPageFlag) != 0 {
		return "buckets"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// leafPageElement retrieves the leaf node by index
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[maxNodesPerPage]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	return ((*[maxNodesPerPage]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[maxNodesPerPage]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	return ((*[maxNodesPerPage]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
type branchPageElement struct {
	pos    uint32
	ksize  uint32
	pageID pageID
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos : n.pos+n.ksize]
}

// leafPageElement represents a node on a leaf page.
type leafPageElement struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos : n.pos+n.ksize]
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos+n.ksize : n.pos+n.ksize+n.vsize]
}
