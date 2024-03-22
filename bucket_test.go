package toyboltdb

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Ensure that a buckets page can set a bucket.
func TestBucketsPut(t *testing.T) {
	b := &buckets{bucketMap: make(map[string]*bucket)}
	b.put("foo", &bucket{rootPageID: 2})
	b.put("bar", &bucket{rootPageID: 3})
	b.put("foo", &bucket{rootPageID: 4})
	assert.Equal(t, len(b.bucketMap), 2)
	assert.Equal(t, b.get("foo").rootPageID, pageID(4))
	assert.Equal(t, b.get("bar").rootPageID, pageID(3))
	assert.Nil(t, b.get("no_such_bucket"))
}

// Ensure that a buckets page can deserialize from a page.
func TestBucketsRead(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.count = 2

	// Insert 2 items at the beginning.
	s := (*[3]bucket)(unsafe.Pointer(&page.ptr))
	s[0] = bucket{rootPageID: 3}
	s[1] = bucket{rootPageID: 4}

	// Write data for the nodes at the end.
	data := (*[4096]byte)(unsafe.Pointer(&s[2]))
	data[0] = 3
	copy(data[1:], []byte("bar"))
	data[4] = 10
	copy(data[5:], []byte("helloworld"))

	// Deserialize page into a buckets page.
	b := &buckets{bucketMap: make(map[string]*bucket)}
	b.read(page)

	// Check that there are two items with correct data.
	assert.Equal(t, len(b.bucketMap), 2)
	assert.Equal(t, b.get("bar").rootPageID, pageID(3))
	assert.Equal(t, b.get("helloworld").rootPageID, pageID(4))
}

// Ensure that a buckets page can serialize itself.
func TestBucketsWrite(t *testing.T) {
	b := &buckets{bucketMap: make(map[string]*bucket)}
	b.put("foo", &bucket{rootPageID: 2})
	b.put("bar", &bucket{rootPageID: 3})

	// Write it to a page.
	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	b.write(p)

	// Read the page back in.
	b2 := &buckets{bucketMap: make(map[string]*bucket)}
	b2.read(p)

	// Check that the two pages are the same.
	assert.Equal(t, len(b.bucketMap), 2)
	assert.Equal(t, b.get("foo").rootPageID, pageID(2))
	assert.Equal(t, b.get("bar").rootPageID, pageID(3))
}
