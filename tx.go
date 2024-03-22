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
}

// txID represents the internal transaction identifier.
type txID uint64

// Close closes the transaction and releases any pages it is using.
func (t *Transaction) Close() {
}

// Bucket retrieves a bucket by name.
// Returns nil if the bucket does not exist.
func (t *Transaction) Bucket(name string) *Bucket {
	return nil
}

// Buckets retrieves a list of all buckets.
func (t *Transaction) Buckets() []*Bucket {
	return nil
}

// Get retrieves the value for a key in a named bucket.
// Returns a nil value if the key does not exist.
// Returns an error if the bucket does not exist.
func (t *Transaction) Get(name string, key []byte) (value []byte, err error) {
	return nil, nil
}

// ForEach executes a function for each key/value pair in a bucket.
// An error is returned if the bucket cannot be found.
func (t *Transaction) ForEach(name string, fn func(k, v []byte) error) error {
	return nil
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary bufferred page is returned.
func (t *Transaction) page(id pageID) *page {
	// TODO
	return nil
}
