package toyboltdb

import (
	"fmt"
	"os"
)

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {
	os      _os
	syscall _syscall
	path    string
}

func (db *DB) Path() string {
	return db.path
}

func (db *DB) GoString() string {
	return fmt.Sprintf("mybolt.DB{path:%q}", db.path)
}

func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open opens a data file at the given path and initializes the database.
// If the file does not exist then it will be created automatically.
//
// Open(): Initializes the reference to the database.
// It's responsible for creating the database if it doesn't exist, obtaining an exclusive lock on the file,
// reading the meta pages, & memory-mapping the file.
//
// - read or create a (new) file
// - read or create meta0, meta1, freelist, (empty leaf) bucket pages
// - mmap
// - reference the above pages to the db
func (db *DB) Open(path string, mode os.FileMode) error {
	return nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() {
}

// Update executes a function within the context of a RWTransaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
func (db *DB) Update(fn func(*RWTransaction) error) error {
	return nil
}

// View executes a function within the context of a Transaction.
// Any error that is returned from the function is returned from the View() method.
func (db *DB) View(fn func(*Transaction) error) error {
	return nil
}
