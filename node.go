// The underlying page and the page's parent pages into memory as "nodes".
// It's like a row, same as pageElement. We use the B+tree, so the "node" exists.
//
// These nodes are where mutations occur during read-write transactions.
// These changes get flushed to disk during commit.
package toyboltdb

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
type node struct {
	transaction *RWTransaction
	isLeaf      bool
	unbalanced  bool
	key         []byte
	depth       int
	pageID      pageID
	parent      *node
	children    inodes
}

// size returns the size of the node after serialization.
func (n *node) size() int {
	var elementSize = n.pageElementSize()

	var size = pageHeaderSize
	for _, item := range n.children {
		size += elementSize + len(item.key) + len(item.value)
	}
	return size
}

// pageElementSize returns the size of each page element based on the type of node.
func (n *node) pageElementSize() int {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// root returns the root node in the tree.
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// childAt returns the child node at a given index.
func (n *node) childAt(index int) *node {
	if n.isLeaf {
		panic(fmt.Sprintf("assertion failed: invalid childAt(%d) on a leaf node", index))
	}
	return n.transaction.node(n.children[index].pageID, n)
}

// put inserts a key/value.
func (n *node) put(oldKey, newKey, value []byte, pageID pageID) {
	// Find insertion index.
	index := sort.Search(len(n.children), func(i int) bool { return bytes.Compare(n.children[i].key, oldKey) != -1 })

	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	exact := (len(n.children) > 0 && index < len(n.children) && bytes.Equal(n.children[index].key, oldKey))
	if !exact {
		n.children = append(n.children, inode{})
		copy(n.children[index+1:], n.children[index:])
	}

	inode := &n.children[index]
	inode.key = newKey
	inode.value = value
	inode.pageID = pageID
}

// del removes a key from the node.
func (n *node) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.children), func(i int) bool { return bytes.Compare(n.children[i].key, key) != -1 })

	// Exit if the key isn't found.
	if index >= len(n.children) || !bytes.Equal(n.children[index].key, key) {
		return
	}

	// Delete inode from the node.
	n.children = append(n.children[:index], n.children[index+1:]...)

	// Mark the node as needing rebalancing.
	n.unbalanced = true
}

// read initializes the node from a page.
func (n *node) read(p *page) {
	n.pageID = p.id
	n.isLeaf = ((p.flags & leafPageFlag) != 0)
	n.children = make(inodes, int(p.count))

	for i := 0; i < int(p.count); i++ {
		inode := &n.children[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pageID = elem.pageID
			inode.key = elem.key()
		}
	}

	// Save first key so we can find the node in the parent when we spill.
	if len(n.children) > 0 {
		n.key = n.children[0].key
	} else {
		n.key = nil
	}
}

// write writes the items onto one or more pages.
func (n *node) write(p *page) {
	// Initialize page.
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}
	p.count = uint16(len(n.children))

	// Loop over each item and write it to the page.
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.children):]
	for i, item := range n.children {
		// Write the page element.
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pageID = item.pageID
		}

		// Write data for the element to the end of the page.
		copy(b[0:], item.key)
		b = b[len(item.key):]
		copy(b[0:], item.value)
		b = b[len(item.value):]
	}
}

// split divides up the node into appropriately sized nodes.
func (n *node) split(pageSize int) []*node {
	// Ignore the split if the page doesn't have at least enough nodes for
	// multiple pages or if the data can fit on a single page.
	if len(n.children) <= (minKeysPerPage*2) || n.size() < pageSize {
		return []*node{n}
	}

	// TODO
	return nil
}

// nodesByDepth sorts a list of branches by deepest first.
type nodesByDepth []*node

func (s nodesByDepth) Len() int           { return len(s) }
func (s nodesByDepth) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nodesByDepth) Less(i, j int) bool { return s[i].depth > s[j].depth }

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or
// point to an element which hasn't been added to a page yet.
type inode struct {
	pageID pageID
	key    []byte
	value  []byte
}

type inodes []inode
