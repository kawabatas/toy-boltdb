package toyboltdb

import (
	"os"
	"testing"
)

func init() {
	testing.Init()
	os.RemoveAll("/tmp/myboltdb")
	os.MkdirAll("/tmp/myboltdb", 0777)
}

// func TestExampleDB_Put(t *testing.T) {
// 	// Open the database.
// 	var db DB
// 	if err := db.Open("/tmp/myboltdb/db_put.db", 0666); err != nil {
// 		t.Fatal(err)
// 	}
// 	defer db.Close()

// 	// Execute several commands within a write transaction.
// 	err := db.Update(func(tx *RWTransaction) error {
// 		if err := tx.CreateBucket("widgets"); err != nil {
// 			return err
// 		}
// 		if err := tx.Put("widgets", []byte("foo"), []byte("bar")); err != nil {
// 			return err
// 		}
// 		return nil
// 	})
// 	// If our transactional block didn't return an error then our data is saved.
// 	if err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	// Access data from within a read-only transactional block.
// 	if err := db.View(func(tx *Transaction) error {
// 		v, err := tx.Get("widgets", []byte("foo"))
// 		fmt.Printf("The value of 'foo' is: %s\n", string(v))
// 		assert.Equal(t, string(v), "bar")
// 		return err
// 	}); err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	// Output:
// 	// The value of 'foo' is: bar
// }

// func TestExampleDB_Delete(t *testing.T) {
// 	var db DB
// 	if err := db.Open("/tmp/myboltdb/db_delete.db", 0666); err != nil {
// 		t.Fatal(err)
// 	}
// 	defer db.Close()

// 	err := db.Update(func(tx *RWTransaction) error {
// 		if err := tx.CreateBucket("widgets"); err != nil {
// 			return err
// 		}
// 		if err := tx.Put("widgets", []byte("foo"), []byte("bar")); err != nil {
// 			return err
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	// Retrieve the key back from the database and verify it.
// 	if err := db.View(func(tx *Transaction) error {
// 		v, err := tx.Get("widgets", []byte("foo"))
// 		fmt.Printf("The value of 'foo' was: %s\n", string(v))
// 		assert.Equal(t, string(v), "bar")
// 		return err
// 	}); err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	err = db.Update(func(tx *RWTransaction) error {
// 		// Delete the "foo" key.
// 		if err := tx.Delete("widgets", []byte("foo")); err != nil {
// 			return err
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		t.Fatal("error occurred")
// 	}
// 	// Retrieve the key back from the database and verify it.
// 	if err := db.View(func(tx *Transaction) error {
// 		v, err := tx.Get("widgets", []byte("foo"))
// 		if v == nil {
// 			fmt.Printf("The value of 'foo' is now: nil\n")
// 		}
// 		assert.Nil(t, v)
// 		return err
// 	}); err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	// Output:
// 	// The value of 'foo' was: bar
// 	// The value of 'foo' is now: nil
// }

// func TestExampleDB_ForEach(t *testing.T) {
// 	testdata := map[string]string{
// 		"dog":   "fun",
// 		"cat":   "lame",
// 		"liger": "awesome",
// 	}

// 	var db DB
// 	if err := db.Open("/tmp/myboltdb/db_foreach.db", 0666); err != nil {
// 		t.Fatal(err)
// 	}
// 	defer db.Close()

// 	err := db.Update(func(tx *RWTransaction) error {
// 		// Insert data into a bucket.
// 		if err := tx.CreateBucket("animals"); err != nil {
// 			return err
// 		}
// 		for k, v := range testdata {
// 			if err := tx.Put("animals", []byte(k), []byte(v)); err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	// Iterate over items in sorted key order.
// 	if err := db.View(func(tx *Transaction) error {
// 		count := 0
// 		tx.ForEach("animals", func(k, v []byte) error {
// 			count++
// 			fmt.Printf("A %s is %s.\n", string(k), string(v))
// 			assert.Equal(t, testdata[string(k)], string(v))
// 			return nil
// 		})
// 		assert.Equal(t, count, len(testdata))
// 		return nil
// 	}); err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	// Output:
// 	// A cat is lame.
// 	// A dog is fun.
// 	// A liger is awesome.
// }

// func TestExample_RWTransaction_Rollback(t *testing.T) {
// 	var db DB
// 	if err := db.Open("/tmp/myboltdb/rwtransaction_rollback.db", 0666); err != nil {
// 		t.Fatal(err)
// 	}
// 	defer db.Close()

// 	err := db.Update(func(tx *RWTransaction) error {
// 		if err := tx.CreateBucket("widgets"); err != nil {
// 			return err
// 		}
// 		if err := tx.Put("widgets", []byte("foo"), []byte("bar")); err != nil {
// 			return err
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	_ = db.Update(func(tx *RWTransaction) error {
// 		// Update the key but rollback the transaction so it never saves.
// 		if err := tx.Put("widgets", []byte("foo"), []byte("baz")); err != nil {
// 			return err
// 		}
// 		// return Err
// 		return ErrValueTooLarge
// 	})
// 	// Ensure that our original value is still set.
// 	if err := db.View(func(tx *Transaction) error {
// 		v, err := tx.Get("widgets", []byte("foo"))
// 		fmt.Printf("The value for 'foo' is still: %s\n", string(v))
// 		assert.Equal(t, string(v), "bar")
// 		return err
// 	}); err != nil {
// 		t.Fatal("error occurred")
// 	}

// 	// Output:
// 	// The value for 'foo' is still: bar
// }
