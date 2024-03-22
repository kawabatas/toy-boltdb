// Tx GET:
// db -> tx -> bucket -> cursor
package toyboltdb

// Transaction represents a read-only transaction on the database.
// It can be used for retrieving values for keys as well as creating cursors for
// iterating over the data.
//
// IMPORTANT: You must close transactions when you are done with them. Pages
// can not be reclaimed by the writer until no more transactions are using them.
// A long running read transaction can cause the database to quickly grow.
type Transaction struct {
	db      *DB
	meta    *meta // copy
	buckets *buckets
	pages   map[pageID]*page // cache
}

// txID represents the internal transaction identifier.
type txID uint64

// init initializes the transaction and associates it with a database.
func (t *Transaction) init(db *DB) {
	t.db = db
	t.pages = nil

	// Copy the meta page since it can be changed by the writer.
	t.meta = &meta{}
	db.meta().copy(t.meta)

	// Read in the buckets page.
	//
	// A page has many buckets, thus transactions.
	t.buckets = &buckets{}
	t.buckets.read(t.page(t.meta.bucketsPageID))
}

// Close closes the transaction and releases any pages it is using.
func (t *Transaction) Close() {
	t.db.txEnd(t)
}

// Bucket retrieves a bucket by name.
// Returns nil if the bucket does not exist.
func (t *Transaction) Bucket(name string) *Bucket {
	b := t.buckets.get(name)
	if b == nil {
		return nil
	}

	return &Bucket{
		bucket:      b,
		name:        name,
		transaction: t,
	}
}

// Buckets retrieves a list of all buckets.
func (t *Transaction) Buckets() []*Bucket {
	buckets := make([]*Bucket, 0, len(t.buckets.bucketMap))
	for name, b := range t.buckets.bucketMap {
		bucket := &Bucket{bucket: b, transaction: t, name: name}
		buckets = append(buckets, bucket)
	}
	return buckets
}

// Get retrieves the value for a key in a named bucket.
// Returns a nil value if the key does not exist.
// Returns an error if the bucket does not exist.
func (t *Transaction) Get(name string, key []byte) (value []byte, err error) {
	b := t.Bucket(name)
	if b == nil {
		return nil, ErrBucketNotFound
	}
	c := b.Cursor()
	return c.Get(key), nil
}

// ForEach executes a function for each key/value pair in a bucket.
// An error is returned if the bucket cannot be found.
func (t *Transaction) ForEach(name string, fn func(k, v []byte) error) error {
	// Open a cursor on the bucket.
	b := t.Bucket(name)
	if b == nil {
		return ErrBucketNotFound
	}
	c := b.Cursor()

	// Iterate over each key/value pair in the bucket.
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary bufferred page is returned.
func (t *Transaction) page(id pageID) *page {
	// Check the dirty pages first.
	if t.pages != nil {
		if p, ok := t.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	return t.db.page(id)
}
