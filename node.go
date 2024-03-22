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

// minKeys returns the minimum number of inodes this node should have.
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
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

// childIndex returns the index of a given child node.
func (n *node) childIndex(child *node) int {
	index := sort.Search(len(n.children), func(i int) bool { return bytes.Compare(n.children[i].key, child.key) != -1 })
	return index
}

// numChildren returns the number of children.
func (n *node) numChildren() int {
	return len(n.children)
}

// nextSibling returns the next node with the same parent.
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// prevSibling returns the previous node with the same parent.
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
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

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
func (n *node) rebalance() {
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// Ignore if node is above threshold (25%) and has enough keys.
	var threshold = n.transaction.db.pageSize / 4
	if n.size() > threshold && len(n.children) > n.minKeys() {
		return
	}

	// Root node has special handling.
	if n.parent == nil {
		// If root node is a branch and only has one node then collapse it.
		if !n.isLeaf && len(n.children) == 1 {
			// Move child's children up.
			child := n.transaction.nodes[n.children[0].pageID]
			n.isLeaf = child.isLeaf
			n.children = child.children[:]

			// Reparent all child nodes being moved.
			for _, inode := range n.children {
				if child, ok := n.transaction.nodes[inode.pageID]; ok {
					child.parent = n
				}
			}

			// Remove old child.
			child.parent = nil
			delete(n.transaction.nodes, child.pageID)
		}
		return
	}

	if n.parent.numChildren() < 2 {
		panic("assertion failed: parent must have at least 2 children")
	}

	// Destination node is right sibling if idx == 0, otherwise left sibling.
	var target *node
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// If target node has extra nodes then just move one over.
	if target.numChildren() > target.minKeys() {
		if useNextSibling {
			// Reparent and move node.
			if child, ok := n.transaction.nodes[target.children[0].pageID]; ok {
				child.parent = n
			}
			n.children = append(n.children, target.children[0])
			target.children = target.children[1:]

			// Update target key on parent.
			target.parent.put(target.key, target.children[0].key, nil, target.pageID)
			target.key = target.children[0].key
		} else {
			// Reparent and move node.
			if child, ok := n.transaction.nodes[target.children[len(target.children)-1].pageID]; ok {
				child.parent = n
			}
			n.children = append(n.children, inode{})
			copy(n.children[1:], n.children)
			n.children[0] = target.children[len(target.children)-1]
			target.children = target.children[:len(target.children)-1]
		}

		// Update parent key for node.
		n.parent.put(n.key, n.children[0].key, nil, n.pageID)
		n.key = n.children[0].key

		return
	}

	// If both this node and the target node are too small then merge them.
	if useNextSibling {
		// Reparent all child nodes being moved.
		for _, inode := range target.children {
			if child, ok := n.transaction.nodes[inode.pageID]; ok {
				child.parent = n
			}
		}

		// Copy over inodes from target and remove target.
		n.children = append(n.children, target.children...)
		n.parent.del(target.key)
		delete(n.transaction.nodes, target.pageID)
	} else {
		// Reparent all child nodes being moved.
		for _, inode := range n.children {
			if child, ok := n.transaction.nodes[inode.pageID]; ok {
				child.parent = target
			}
		}

		// Copy over inodes to target and remove node.
		target.children = append(target.children, n.children...)
		n.parent.del(n.key)
		n.parent.put(target.key, target.children[0].key, nil, target.pageID)
		delete(n.transaction.nodes, n.pageID)
	}

	// Either this node or the target node was deleted from the parent so rebalance it.
	n.parent.rebalance()
}

// split divides up the node into appropriately sized nodes.
func (n *node) split(pageSize int) []*node {
	// Ignore the split if the page doesn't have at least enough nodes for
	// multiple pages or if the data can fit on a single page.
	if len(n.children) <= (minKeysPerPage*2) || n.size() < pageSize {
		return []*node{n}
	}

	// Set fill threshold to 50%.
	threshold := pageSize / 2

	// Group into smaller pages and target a given fill size.
	size := pageHeaderSize
	inodes := n.children
	current := n
	current.children = nil
	var nodes []*node

	for i, inode := range inodes {
		elemSize := n.pageElementSize() + len(inode.key) + len(inode.value)

		// divide new node
		if len(current.children) >= minKeysPerPage && i < len(inodes)-minKeysPerPage && size+elemSize > threshold {
			size = pageHeaderSize
			nodes = append(nodes, current)
			current = &node{transaction: n.transaction, isLeaf: n.isLeaf}
		}

		size += elemSize
		current.children = append(current.children, inode)
	}
	nodes = append(nodes, current)

	return nodes
}

// dereference causes the node to copy all its inode key/value references to heap memory.
// This is required when the mmap is reallocated so inodes are not pointing to stale data.
func (n *node) dereference() {
	key := make([]byte, len(n.key))
	copy(key, n.key)
	n.key = key

	for i := range n.children {
		inode := &n.children[i]

		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key

		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}
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
