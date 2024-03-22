package toyboltdb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that a bucket can be created and retrieved.
func TestRWTransactionCreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		err := db.Update(func(txn *RWTransaction) error {
			// Create a bucket.
			err := txn.CreateBucket("rw-widgets")
			return err
		})
		assert.NoError(t, err)

		err = db.View(func(txn *Transaction) error {
			// Read the bucket through a separate transaction.
			b := txn.Bucket("rw-widgets")
			assert.NotNil(t, b)
			return nil
		})
		assert.NoError(t, err)
	})
}

// Ensure that a bucket can be created if it doesn't already exist.
func TestRWTransactionCreateBucketIfNotExists(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			assert.NoError(t, txn.CreateBucketIfNotExists("rw-widgets"))
			assert.NoError(t, txn.CreateBucketIfNotExists("rw-widgets"))
			return nil
		})

		_ = db.View(func(txn *Transaction) error {
			// Read the bucket through a separate transaction.
			b := txn.Bucket("rw-widgets")
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a bucket cannot be created twice.
func TestRWTransactionRecreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			// Create a bucket.
			err := txn.CreateBucket("rw-widgets")
			assert.NoError(t, err)

			// Create the same bucket again.
			err = txn.CreateBucket("rw-widgets")
			assert.Equal(t, err, ErrBucketExists)
			return nil
		})
	})
}

// Ensure that a bucket is created with a non-blank name.
func TestRWTransactionCreateBucketWithoutName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			err := txn.CreateBucket("")
			assert.Equal(t, err, ErrBucketNameRequired)
			return err
		})
	})
}

// Ensure that a bucket name is not too long.
func TestRWTransactionCreateBucketWithLongName(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			err := txn.CreateBucket(strings.Repeat("X", 255))
			assert.NoError(t, err)

			err = txn.CreateBucket(strings.Repeat("X", 256))
			assert.Equal(t, err, ErrBucketNameTooLarge)
			return err
		})
	})
}

// Ensure that a bucket can be deleted.
func TestRWTransactionDeleteBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			// Create a bucket and add a value.
			txn.CreateBucket("rw-widgets")
			txn.Put("rw-widgets", []byte("rw-foo"), []byte("rw-bar"))

			// Delete the bucket and make sure we can't get the value.
			assert.NoError(t, txn.DeleteBucket("rw-widgets"))
			value, err := txn.Get("rw-widgets", []byte("foo"))
			assert.Equal(t, err, ErrBucketNotFound)
			assert.Nil(t, value)

			// Create the bucket again and make sure there's not a phantom value.
			assert.NoError(t, txn.CreateBucket("rw-widgets"))
			value, err = txn.Get("rw-widgets", []byte("rw-foo"))
			assert.NoError(t, err)
			assert.Nil(t, value)
			return nil
		})
	})
}

// Ensure that a bucket can return an autoincrementing sequence.
func TestRWTransactionNextSequence(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			txn.CreateBucket("rw-widgets")
			txn.CreateBucket("rw-woojits")

			// Make sure sequence increments.
			seq, err := txn.NextSequence("rw-widgets")
			assert.NoError(t, err)
			assert.Equal(t, seq, 1)
			seq, err = txn.NextSequence("rw-widgets")
			assert.NoError(t, err)
			assert.Equal(t, seq, 2)

			// Buckets should be separate.
			seq, err = txn.NextSequence("rw-woojits")
			assert.NoError(t, err)
			assert.Equal(t, seq, 1)

			// Missing buckets return an error.
			seq, err = txn.NextSequence("no_such_bucket")
			assert.Equal(t, err, ErrBucketNotFound)
			assert.Equal(t, seq, 0)
			return nil
		})
	})
}

// Ensure that an error is returned when inserting into a bucket that doesn't exist.
func TestRWTransactionPutBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			err := txn.Put("rw-widgets", []byte("rw-foo"), []byte("rw-bar"))
			assert.Equal(t, err, ErrBucketNotFound)
			return nil
		})
	})
}

// Ensure that an error is returned when inserting with an empty key.
func TestRWTransactionPutEmptyKey(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			txn.CreateBucket("rw-widgets")
			err := txn.Put("rw-widgets", []byte(""), []byte("rw-bar"))
			assert.Equal(t, err, ErrKeyRequired)
			err = txn.Put("rw-widgets", nil, []byte("rw-bar"))
			assert.Equal(t, err, ErrKeyRequired)
			return nil
		})
	})
}

// Ensure that an error is returned when inserting with a key that's too large.
func TestRWTransactionPutKeyTooLarge(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			txn.CreateBucket("rw-widgets")
			err := txn.Put("rw-widgets", make([]byte, 32769), []byte("rw-bar"))
			assert.Equal(t, err, ErrKeyTooLarge)
			return nil
		})
	})
}

// Ensure that an error is returned when deleting from a bucket that doesn't exist.
func TestRWTransactionDeleteBucketNotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			err := txn.DeleteBucket("rw-widgets")
			assert.Equal(t, err, ErrBucketNotFound)
			return nil
		})
	})
}
