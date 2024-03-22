package toyboltdb

import (
	"sort"
	"unsafe"
)

// Bucket represents a collection of key/value pairs inside the database.
// A bucket is simply a named collection of key/value pairs, just like Go’s map.
// It's also like a **table**.
//
// All keys inside the bucket are unique.
// The Bucket type is not typically used directly.
// Instead the bucket name is typically passed into the Get(), Put(), or Delete() functions.
type Bucket struct {
	*bucket
	name        string
	transaction *Transaction
}

// bucket represents the **on-file** representation of a bucket.
type bucket struct {
	rootPageID pageID
	sequence   uint64
}

// Name returns the name of the bucket.
func (b *Bucket) Name() string {
	return b.name
}

// Cursor creates a new cursor for this bucket.
func (b *Bucket) Cursor() *Cursor {
	return &Cursor{
		transaction: b.transaction,
		rootPageID:  b.rootPageID,
		stack:       make([]pageElementRef, 0),
	}
}

// buckets represents a **in-memory** buckets page.
//
// A page has many buckets
type buckets struct {
	pageID    pageID
	bucketMap map[string]*bucket
}

// size returns the size of the page after serialization.
func (b *buckets) size() int {
	var size = pageHeaderSize
	for key := range b.bucketMap {
		size += int(unsafe.Sizeof(bucket{})) + len(key)
	}
	return size
}

// get retrieves a bucket by name.
func (b *buckets) get(key string) *bucket {
	return b.bucketMap[key]
}

// put sets a new value for a bucket.
func (b *buckets) put(key string, bc *bucket) {
	b.bucketMap[key] = bc
}

// del deletes a bucket by name.
func (b *buckets) del(key string) {
	if bc := b.bucketMap[key]; bc != nil {
		delete(b.bucketMap, key)
	}
}

// read initializes the data **from** an on-disk **page**.
//
// page.ptr
//
//	| buckets[0]              | buckets[1]              |
//	| key size    | key value | key size    | key value |...
func (b *buckets) read(p *page) {
	b.pageID = p.id
	b.bucketMap = make(map[string]*bucket)

	var bucketMap []*bucket
	var keys []string

	// Read items.
	nodes := (*[maxNodesPerPage]bucket)(unsafe.Pointer(&p.ptr))
	for i := 0; i < int(p.count); i++ {
		node := &nodes[i]
		bucketMap = append(bucketMap, node)
	}

	// Read keys.
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(&nodes[p.count]))[:]
	for i := 0; i < int(p.count); i++ {
		size := int(buf[0])
		buf = buf[1:]
		keys = append(keys, string(buf[:size]))
		buf = buf[size:]
	}

	// Associate keys and items.
	for index, key := range keys {
		b.bucketMap[key] = &bucket{
			rootPageID: bucketMap[index].rootPageID,
			sequence:   bucketMap[index].sequence,
		}
	}
}

// write writes the items **onto** a **page**.
//
// page.ptr
//
//	| buckets[0]              | buckets[1]              |
//	| key size    | key name  | key size     | key name |...
func (b *buckets) write(p *page) {
	// Initialize page.
	p.flags |= bucketsPageFlag
	p.count = uint16(len(b.bucketMap))

	// Sort keys.
	var keys []string
	for key := range b.bucketMap {
		keys = append(keys, key)
	}
	sort.StringSlice(keys).Sort()

	// Write each bucket(item) to the page.
	buckets := (*[maxNodesPerPage]bucket)(unsafe.Pointer(&p.ptr))
	for index, key := range keys {
		buckets[index] = *b.bucketMap[key]
	}

	// Write each key to the page.
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(&buckets[p.count]))[:]
	for _, key := range keys {
		// size
		buf[0] = byte(len(key))
		buf = buf[1:]
		// key name
		copy(buf, []byte(key))
		buf = buf[len(key):]
	}
}

// updateRootPageID finds a bucket by root id and then updates it to point to a new root.
func (b *buckets) updateRootPageID(oldid, newid pageID) {
	for _, b := range b.bucketMap {
		if b.rootPageID == oldid {
			b.rootPageID = newid
			return
		}
	}
}
