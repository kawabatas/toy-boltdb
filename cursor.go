package toyboltdb

// Cursor:
// This object is simply for traversing the B+tree of on-disk pages or in-memory nodes.
// It can seek to a specific key, move to the first or last value, or it can move forward or backward.
// The cursor handles the movement up and down the B+tree transparently to the end user.
//
// Cursor represents an iterator that can traverse over all key/value pairs in a **bucket** in sorted order.
// Cursors can be obtained from a Transaction and are valid as long as the Transaction is open.
type Cursor struct {
}

// First moves the cursor to the first item in the bucket and returns its key and value.
// If the bucket is empty then a nil key is returned.
func (c *Cursor) First() (key []byte, value []byte) {
	return nil, nil
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
// If the cursor is at the end of the bucket then a nil key returned.
func (c *Cursor) Next() (key []byte, value []byte) {
	return nil, nil
}

// Get moves the cursor to a given key and returns its value.
// If the key does not exist then the cursor is left at the closest key and a nil key is returned.
func (c *Cursor) Get(key []byte) (value []byte) {
	return nil
}
