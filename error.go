package toyboltdb

import "errors"

var (
	// ErrInvalid is returned when a data file is not a Bolt-formatted database.
	ErrInvalid = errors.New("invalid database")

	// ErrVersionMismatch is returned when the data file was created with a
	// different version of Bolt.
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrDatabaseNotOpen is returned when a DB instance is accessed before it
	// is opened or after it is closed.
	ErrDatabaseNotOpen = errors.New("database not open")

	// ErrDatabaseOpen is returned when opening a database that is
	// already open.
	ErrDatabaseOpen = errors.New("database already open")

	// ErrBucketNotFound is returned when trying to access a bucket that has
	// not been created yet.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrBucketExists is returned when creating a bucket that already exists.
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNameRequired is returned when creating a bucket with a blank name.
	ErrBucketNameRequired = errors.New("bucket name required")

	// ErrBucketNameTooLarge is returned when creating a bucket with a name
	// that is longer than MaxBucketNameSize.
	ErrBucketNameTooLarge = errors.New("bucket name too large")

	// ErrKeyRequired is returned when inserting a zero-length key.
	ErrKeyRequired = errors.New("key required")

	// ErrKeyTooLarge is returned when inserting a key that is larger than MaxKeySize.
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge is returned when inserting a value that is larger than MaxValueSize.
	ErrValueTooLarge = errors.New("value too large")
)
