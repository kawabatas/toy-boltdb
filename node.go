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

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or
// point to an element which hasn't been added to a page yet.
type inode struct {
	pageID pageID
	key    []byte
	value  []byte
}

type inodes []inode
