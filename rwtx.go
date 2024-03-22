// RWTx CreateBucket:
// db -> tx -> (page)(freelist allocate) -> bucket -> put to bucket obj
//
// RWTx PUT:
// db -> tx -> bucket -> cursor -> node -> put to node obj
//
// RWTx Commit:
// db -> tx -> (page)(freelist allocate, current size) -> write (node and bucket) obj to page
// -> write page to disk -> write meta to page and disk
package toyboltdb

import (
	"sort"
	"unsafe"
)

const (
	MaxKeySize        = 32768      // 16bit
	MaxValueSize      = 4294967295 // 32bit
	MaxBucketNameSize = 255        // 8bit
)

// RWTransaction represents a transaction that can read and write data.
// Only one read/write transaction can be active for a database at a time.
// RWTransaction is composed of a read-only Transaction so it can also use
// functions provided by Transaction.
type RWTransaction struct {
	Transaction
	nodes   map[pageID]*node // cache
	pending []*node
}

// init initializes the transaction.
func (t *RWTransaction) init(db *DB) {
	t.Transaction.init(db)
	t.pages = make(map[pageID]*page)

	// Increment the transaction id.
	t.meta.txID += txID(1)
}

// Commit writes all changes to **disk** and updates the **meta page**.
// Returns an error if a disk write error occurs.
func (t *RWTransaction) Commit() error {
	defer t.db.rwtxEnd()

	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// Rebalance and spill data onto dirty pages.
	t.rebalance()
	t.spill()

	// Spill buckets page.
	p, err := t.allocate((t.buckets.size() / t.db.pageSize) + 1)
	if err != nil {
		return err
	}
	t.buckets.write(p)

	// Write dirty pages to disk.
	if err := t.write(); err != nil {
		return err
	}

	// Update the meta.
	t.meta.bucketsPageID = p.id

	// Write meta to disk.
	if err := t.writeMeta(); err != nil {
		return err
	}

	return nil
}

// Rollback closes the transaction and ignores all previous updates.
func (t *RWTransaction) Rollback() {
	t.db.rwtxEnd()
}

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
func (t *RWTransaction) CreateBucket(name string) error {
	// Check if bucket already exists.
	if b := t.Bucket(name); b != nil {
		return ErrBucketExists
	} else if len(name) == 0 {
		return ErrBucketNameRequired
	} else if len(name) > MaxBucketNameSize {
		return ErrBucketNameTooLarge
	}

	// Create a blank root leaf page.
	p, err := t.allocate(1)
	if err != nil {
		return err
	}
	p.flags = leafPageFlag

	// Add bucket to buckets page.
	t.buckets.put(name, &bucket{rootPageID: p.id})
	return nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
func (t *RWTransaction) CreateBucketIfNotExists(name string) error {
	err := t.CreateBucket(name)
	if err != nil && err != ErrBucketExists {
		return err
	}
	return nil
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found.
func (t *RWTransaction) DeleteBucket(name string) error {
	if b := t.Bucket(name); b == nil {
		return ErrBucketNotFound
	}

	// Remove from buckets page.
	t.buckets.del(name)

	// TODO(benbjohnson): Free all pages.

	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
func (t *RWTransaction) NextSequence(name string) (int, error) {
	// Check if bucket already exists.
	b := t.Bucket(name)
	if b == nil {
		return 0, ErrBucketNotFound
	}

	// Increment and return the sequence.
	b.bucket.sequence++

	return int(b.bucket.sequence), nil
}

// Put sets the value for a key inside of the named bucket.
// If the key exist then its previous value will be overwritten.
// Returns an error if the bucket is not found, if the key is blank, if the key is too large, or if the value is too large.
func (t *RWTransaction) Put(name string, key []byte, value []byte) error {
	b := t.Bucket(name)
	if b == nil {
		return ErrBucketNotFound
	}

	// Validate the key and data size.
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Move cursor to correct position.
	c := b.Cursor()
	c.Get(key)

	// Insert the key/value.
	c.node(t).put(key, key, value, 0)

	return nil
}

// Delete removes a key from the named bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket cannot be found.
func (t *RWTransaction) Delete(name string, key []byte) error {
	b := t.Bucket(name)
	if b == nil {
		return ErrBucketNotFound
	}

	// Move cursor to correct position.
	c := b.Cursor()
	c.Get(key)

	// Delete the node if we have a matching key.
	c.node(t).del(key)

	return nil
}

// allocate returns a contiguous block of memory starting at a given page.
func (t *RWTransaction) allocate(count int) (*page, error) {
	p, err := t.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// Save to our page cache.
	t.pages[p.id] = p

	return p, nil
}

// node creates a node from a page and associates it with a given parent.
func (t *RWTransaction) node(pageID pageID, parent *node) *node {
	// Retrieve node if it has already been fetched.
	if n := t.nodes[pageID]; n != nil {
		return n
	}

	// Otherwise create a branch and cache it.
	n := &node{transaction: t, parent: parent}
	if n.parent != nil {
		n.depth = n.parent.depth + 1
	}
	n.read(t.page(pageID))
	t.nodes[pageID] = n

	return n
}

// rebalance attempts to balance all nodes.
func (t *RWTransaction) rebalance() {
	for _, n := range t.nodes {
		n.rebalance()
	}
}

// spill writes all the nodes to dirty pages.
func (t *RWTransaction) spill() error {
	// Keep track of the current root nodes.
	// We will update this at the end once all nodes are created.
	type root struct {
		node   *node
		pageID pageID
	}
	var roots []root

	// Sort nodes by highest depth first.
	nodes := make(nodesByDepth, 0, len(t.nodes))
	for _, n := range t.nodes {
		nodes = append(nodes, n)
	}
	sort.Sort(nodes)

	// Spill nodes by deepest first.
	for i := 0; i < len(nodes); i++ {
		n := nodes[i]

		// Save existing root buckets for later.
		if n.parent == nil && n.pageID != 0 {
			roots = append(roots, root{n, n.pageID})
		}

		// Split nodes into appropriate sized nodes.
		// The first node in this list will be a reference to n to preserve ancestry.
		newNodes := n.split(t.db.pageSize)
		t.pending = newNodes

		// If this is a root node that split then create a parent node.
		if n.parent == nil && len(newNodes) > 1 {
			n.parent = &node{transaction: t, isLeaf: false}
			nodes = append(nodes, n.parent)
		}

		// Add node's page to the freelist.
		if n.pageID > 0 {
			t.db.freelist.free(t.meta.txID, t.page(n.pageID))
		}

		// Write nodes to dirty pages.
		for i, newNode := range newNodes {
			// Allocate contiguous space for the node.
			p, err := t.allocate((newNode.size() / t.db.pageSize) + 1)
			if err != nil {
				return err
			}

			// Write the node to the page.
			newNode.write(p)
			newNode.pageID = p.id
			newNode.parent = n.parent

			// The first node should use the existing entry, other nodes are inserts.
			var oldKey []byte
			if i == 0 {
				oldKey = n.key
			} else {
				oldKey = newNode.children[0].key
			}

			// Update the parent entry.
			if newNode.parent != nil {
				newNode.parent.put(oldKey, newNode.children[0].key, nil, newNode.pageID)
			}
		}

		t.pending = nil
	}

	// Update roots with new roots.
	for _, root := range roots {
		t.buckets.updateRootPageID(root.pageID, root.node.root().pageID)
	}

	// Clear out nodes now that they are all spilled.
	t.nodes = make(map[pageID]*node)

	return nil
}

// write writes any dirty pages to disk.
func (t *RWTransaction) write() error {
	// Sort pages by id.
	pages := make(pages, 0, len(t.pages))
	for _, p := range t.pages {
		pages = append(pages, p)
	}
	sort.Sort(pages)

	// Write pages to disk in order.
	for _, p := range pages {
		size := (int(p.overflow) + 1) * t.db.pageSize
		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:size]
		offset := int64(p.id) * int64(t.db.pageSize)
		if _, err := t.db.file.WriteAt(buf, offset); err != nil {
			return err
		}
	}

	// Clear out page cache.
	t.pages = make(map[pageID]*page)

	return nil
}

// writeMeta writes the meta to the disk.
func (t *RWTransaction) writeMeta() error {
	// Create a temporary buffer for the meta page.
	buf := make([]byte, t.db.pageSize)
	p := t.db.pageInBuffer(buf, 0)
	t.meta.write(p)

	// Write the meta page to file.
	t.db.metafile.WriteAt(buf, int64(p.id)*int64(t.db.pageSize))

	return nil
}

// dereference removes all references to the old mmap.
func (t *RWTransaction) dereference() {
	for _, n := range t.nodes {
		n.dereference()
	}

	for _, n := range t.pending {
		n.dereference()
	}
}
