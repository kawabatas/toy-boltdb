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

// RWTransaction represents a transaction that can read and write data.
// Only one read/write transaction can be active for a database at a time.
// RWTransaction is composed of a read-only Transaction so it can also use
// functions provided by Transaction.
type RWTransaction struct {
}

// Commit writes all changes to **disk** and updates the **meta page**.
// Returns an error if a disk write error occurs.
func (t *RWTransaction) Commit() error {
	return nil
}

// Rollback closes the transaction and ignores all previous updates.
func (t *RWTransaction) Rollback() {
}

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
func (t *RWTransaction) CreateBucket(name string) error {
	return nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
func (t *RWTransaction) CreateBucketIfNotExists(name string) error {
	return nil
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found.
func (t *RWTransaction) DeleteBucket(name string) error {
	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
func (t *RWTransaction) NextSequence(name string) (int, error) {
	return 0, nil
}

// Put sets the value for a key inside of the named bucket.
// If the key exist then its previous value will be overwritten.
// Returns an error if the bucket is not found, if the key is blank, if the key is too large, or if the value is too large.
func (t *RWTransaction) Put(name string, key []byte, value []byte) error {
	return nil
}

// Delete removes a key from the named bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket cannot be found.
func (t *RWTransaction) Delete(name string, key []byte) error {
	return nil
}

// dereference removes all references to the old mmap.
func (t *RWTransaction) dereference() {
	// TODO
}
