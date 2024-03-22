package toyboltdb

import (
	"bytes"
	"fmt"
	"sort"
)

// Cursor:
// This object is simply for traversing the B+tree of on-disk pages or in-memory nodes.
// It can seek to a specific key, move to the first or last value, or it can move forward or backward.
// The cursor handles the movement up and down the B+tree transparently to the end user.
//
// Cursor represents an iterator that can traverse over all key/value pairs in a **bucket** in sorted order.
// Cursors can be obtained from a Transaction and are valid as long as the Transaction is open.
type Cursor struct {
	transaction *Transaction
	rootPageID  pageID
	stack       []pageElementRef
}

// First moves the cursor to the first item in the bucket and returns its key and value.
// If the bucket is empty then a nil key is returned.
func (c *Cursor) First() (key []byte, value []byte) {
	if len(c.stack) > 0 {
		c.stack = c.stack[:0] // delete all elements
	}
	c.stack = append(c.stack, pageElementRef{page: c.transaction.page(c.rootPageID), index: 0})
	c.first()
	return c.keyValue()
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
// If the cursor is at the end of the bucket then a nil key returned.
func (c *Cursor) Next() (key []byte, value []byte) {
	// Attempt to move over one element until we're successful.
	// Move up the stack as we hit the end of each page in our stack.
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index < elem.page.count-1 {
			elem.index++
			break
		}
		c.stack = c.stack[:i]
	}

	// If we've hit the end then return nil.
	if len(c.stack) == 0 {
		return nil, nil
	}

	// Move down the stack to find the first element of the first leaf under this branch.
	c.first()
	return c.keyValue()
}

// Get moves the cursor to a given key and returns its value.
// If the key does not exist then the cursor is left at the closest key and a nil key is returned.
func (c *Cursor) Get(key []byte) (value []byte) {
	// Start from root page and traverse to correct page.
	c.stack = c.stack[:0] // delete all elements
	c.search(key, c.transaction.page(c.rootPageID))
	p, index := c.top()

	// If the cursor is pointing to the end of page then return nil.
	if index == p.count {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	if !bytes.Equal(key, c.leafElement().key()) {
		return nil
	}

	return c.leafElement().value()
}

// first moves the cursor to the first leaf element under the last page in the stack.
func (c *Cursor) first() {
	p := c.stack[len(c.stack)-1].page
	for {
		// Exit when we hit a leaf page.
		if (p.flags & leafPageFlag) != 0 {
			break
		}

		// Keep adding pages pointing to the first element to the stack.
		p = c.transaction.page(p.branchPageElement(c.stack[len(c.stack)-1].index).pageID)
		c.stack = append(c.stack, pageElementRef{page: p, index: 0})
	}
}

// keyValue returns the key and value of the current leaf element.
func (c *Cursor) keyValue() ([]byte, []byte) {
	ref := &c.stack[len(c.stack)-1]
	if ref.index >= ref.page.count {
		return nil, nil
	}
	e := ref.page.leafPageElement(ref.index)
	return e.key(), e.value()
}

// top returns the page and leaf node that the cursor is currently pointing at.
func (c *Cursor) top() (*page, uint16) {
	ptr := c.stack[len(c.stack)-1]
	return ptr.page, ptr.index
}

// element returns the leaf element that the cursor is currently positioned on.
func (c *Cursor) leafElement() *leafPageElement {
	ref := c.stack[len(c.stack)-1]
	return ref.page.leafPageElement(ref.index)
}

// search recursively performs a binary search against a given page until it finds a given key.
func (c *Cursor) search(key []byte, p *page) {
	if (p.flags & (branchPageFlag | leafPageFlag)) == 0 {
		panic(fmt.Sprintf("assertion failed: invalid page type: %s", p.typ()))
	}
	e := pageElementRef{page: p}
	c.stack = append(c.stack, e)

	// If we're on a leaf page then find the specific node.
	if (p.flags & leafPageFlag) != 0 {
		c.nsearch(key, p)
		return
	}

	// Binary search for the correct range.
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	// false
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = uint16(index)

	// Recursively search to the next page.
	c.search(key, c.transaction.page(inodes[index].pageID))
}

// nsearch searches a leaf node for the index of the node that matches key.
func (c *Cursor) nsearch(key []byte, p *page) {
	e := &c.stack[len(c.stack)-1]

	// Binary search for the correct leaf node index.
	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = uint16(index)
}

// node returns the node that the cursor is currently positioned on.
func (c *Cursor) node(t *RWTransaction) *node {
	if len(c.stack) == 0 {
		panic("assertion failed: accessing a node with a zero-length cursor stack")
	}

	// Start from root and traverse down the hierarchy.
	n := t.node(c.stack[0].page.id, nil)
	for _, ref := range c.stack[:len(c.stack)-1] {
		if n.isLeaf {
			panic("assertion failed: expected branch node")
		}
		if ref.page.id != n.pageID {
			panic(fmt.Sprintf("assertion failed: node/page mismatch a: %d != %d", ref.page.id, n.childAt(int(ref.index)).pageID))
		}
		n = n.childAt(int(ref.index))
	}
	if !n.isLeaf {
		panic("assertion failed: expected leaf node")
	}
	if n.pageID != c.stack[len(c.stack)-1].page.id {
		panic(fmt.Sprintf("assertion failed: node/page mismatch b: %d != %d", n.pageID, c.stack[len(c.stack)-1].page.id))
	}
	return n
}
