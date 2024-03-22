package toyboltdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that the database can retrieve a list of buckets.
func TestTransactionBuckets(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			txn.CreateBucket("foo")
			txn.CreateBucket("bar")
			txn.CreateBucket("baz")
			return nil
		})

		_ = db.View(func(txn *Transaction) error {
			buckets := txn.Buckets()
			if assert.Equal(t, len(buckets), 3) {
				assert.Equal(t, buckets[0].Name(), "bar")
				assert.Equal(t, buckets[1].Name(), "baz")
				assert.Equal(t, buckets[2].Name(), "foo")
			}
			return nil
		})
	})
}

// Ensure that a Transaction retrieving a non-existent key returns nil.
func TestTransactionGetMissing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		_ = db.Update(func(txn *RWTransaction) error {
			txn.CreateBucket("widgets")
			txn.Put("widgets", []byte("foo"), []byte("bar"))
			return nil
		})

		_ = db.View(func(txn *Transaction) error {
			value, err := txn.Get("widgets", []byte("no_such_key"))
			assert.NoError(t, err)
			assert.Nil(t, value)
			return nil
		})
	})
}
