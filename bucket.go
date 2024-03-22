package toyboltdb

// Bucket represents a collection of key/value pairs inside the database.
// A bucket is simply a named collection of key/value pairs, just like Go’s map.
// It's also like a **table**.
//
// All keys inside the bucket are unique.
// The Bucket type is not typically used directly.
// Instead the bucket name is typically passed into the Get(), Put(), or Delete() functions.
type Bucket struct {
}

// Name returns the name of the bucket.
func (b *Bucket) Name() string {
	return ""
}

// Cursor creates a new cursor for this bucket.
func (b *Bucket) Cursor() *Cursor {
	return nil
}
