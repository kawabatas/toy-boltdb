package toyboltdb

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Ensure that a node can insert a key/value.
func TestNodePut(t *testing.T) {
	n := &node{children: make(inodes, 0)}
	n.put([]byte("baz"), []byte("baz"), []byte("2"), 0)
	n.put([]byte("foo"), []byte("foo"), []byte("0"), 0)
	n.put([]byte("bar"), []byte("bar"), []byte("1"), 0)
	n.put([]byte("foo"), []byte("foo"), []byte("3"), 0)
	assert.Equal(t, len(n.children), 3)
	assert.Equal(t, n.children[0].key, []byte("bar"))
	assert.Equal(t, n.children[0].value, []byte("1"))
	assert.Equal(t, n.children[1].key, []byte("baz"))
	assert.Equal(t, n.children[1].value, []byte("2"))
	assert.Equal(t, n.children[2].key, []byte("foo"))
	assert.Equal(t, n.children[2].value, []byte("3"))
}

// Ensure that a node can deserialize from a leaf page.
func TestNodeReadLeafPage(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = leafPageFlag
	page.count = 2

	// Insert 2 elements at the beginning. sizeof(leafPageElement) == 16
	nodes := (*[3]leafPageElement)(unsafe.Pointer(&page.ptr))
	nodes[0] = leafPageElement{flags: 0, pos: 32, ksize: 3, vsize: 4}  // pos = sizeof(leafPageElement)*2
	nodes[1] = leafPageElement{flags: 0, pos: 23, ksize: 10, vsize: 3} // pos = sizeof(leafPageElement) + 3 + 4

	// Write data for the nodes at the end.
	data := (*[4096]byte)(unsafe.Pointer(&nodes[2]))
	copy(data[:], []byte("barfooz"))
	copy(data[7:], []byte("helloworldbye"))

	// Deserialize page into a leaf.
	n := &node{}
	n.read(page)

	// Check that there are two inodes with correct data.
	assert.True(t, n.isLeaf)
	assert.Equal(t, len(n.children), 2)
	assert.Equal(t, n.children[0].key, []byte("bar"))
	assert.Equal(t, n.children[0].value, []byte("fooz"))
	assert.Equal(t, n.children[1].key, []byte("helloworld"))
	assert.Equal(t, n.children[1].value, []byte("bye"))
}
