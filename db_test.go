package toyboltdb

import (
	"io"
	"os"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Ensure that a database can be opened without error.
func TestDBOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		err := db.Open(path, 0666)
		assert.NoError(t, err)
		assert.Equal(t, db.Path(), path)
	})
}

// Ensure that the database returns an error if already open.
func TestDBReopen(t *testing.T) {
	withDB(func(db *DB, path string) {
		db.Open(path, 0666)
		err := db.Open(path, 0666)
		assert.Equal(t, err, ErrDatabaseOpen)
	})
}

// Ensure that the database returns an error if the file handle cannot be open.
func TestDBOpenFileError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		exp := &os.PathError{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return((*mockfile)(nil), exp)
		err := db.Open(path, 0666)
		assert.Equal(t, err, exp)
	})
}

// Ensure that the database returns an error if the meta file handle cannot be open.
func TestDBOpenMetaFileError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		exp := &os.PathError{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(&mockfile{}, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return((*mockfile)(nil), exp)
		err := db.Open(path, 0666)
		assert.Equal(t, err, exp)
	})
}

// Ensure that write errors to the meta file handler during initialization are returned.
func TestDBMetaInitWriteError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		// Mock the file system.
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x10000)
		file.On("Stat").Return(&mockfileinfo{"", 0, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, io.ErrShortWrite)

		// Open the database.
		err := db.Open(path, 0666)
		assert.Equal(t, err, io.ErrShortWrite)
	})
}

// Ensure that a database that is too small returns an error.
func TestDBFileTooSmall(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x1000)
		file.On("Stat").Return(&mockfileinfo{"", 0, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		err := db.Open(path, 0666)
		assert.ErrorContains(t, err, errMsgFileTooSmall)
	})
}

// Ensure that stat errors during mmap get returned.
func TestDBMmapStatError(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		exp := &os.PathError{}
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x1000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return((*mockfileinfo)(nil), exp)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		err := db.Open(path, 0666)
		assert.ErrorContains(t, err, errMsgStat)
	})
}

// Ensure that corrupt meta0 page errors get returned.
func TestDBCorruptMeta0(t *testing.T) {
	withMockDB(func(db *DB, mockos *mockos, mocksyscall *mocksyscall, path string) {
		var m meta
		m.magic = magic
		m.version = version
		m.pageSize = 0x8000

		// Create a file with bad magic.
		b := make([]byte, 0x10000)
		p0, p1 := (*page)(unsafe.Pointer(&b[0x0000])), (*page)(unsafe.Pointer(&b[0x8000]))
		p0.meta().magic = 0
		p0.meta().version = version
		p1.meta().magic = magic
		p1.meta().version = version

		// Mock file access.
		file, metafile := &mockfile{}, &mockfile{}
		mockos.On("OpenFile", path, os.O_RDWR|os.O_CREATE, os.FileMode(0666)).Return(file, nil)
		mockos.On("OpenFile", path, os.O_RDWR|os.O_SYNC, os.FileMode(0666)).Return(metafile, nil)
		mockos.On("Getpagesize").Return(0x10000)
		file.On("ReadAt", mock.Anything, int64(0)).Return(0, nil)
		file.On("Stat").Return(&mockfileinfo{"", 0x10000, 0666, time.Now(), false, nil}, nil)
		metafile.On("WriteAt", mock.Anything, int64(0)).Return(0, nil)
		mocksyscall.On("Mmap", 0, int64(0), 0x10000, syscall.PROT_READ, syscall.MAP_SHARED).Return(b, nil)

		// Open the database.
		err := db.Open(path, 0666)
		assert.ErrorContains(t, err, ErrInvalid.Error())
	})
}

// Ensure that the mmap grows appropriately.
func TestDBMmapSize(t *testing.T) {
	db := &DB{pageSize: 4096}
	assert.Equal(t, db.mmapSize(0), minMmapSize)
	assert.Equal(t, db.mmapSize(16384), minMmapSize)
	assert.Equal(t, db.mmapSize(minMmapSize-1), minMmapSize)
	assert.Equal(t, db.mmapSize(minMmapSize), minMmapSize*2)
	assert.Equal(t, db.mmapSize(10000000), 20000768)
	assert.Equal(t, db.mmapSize((1<<30)-1), 2147483648)
	assert.Equal(t, db.mmapSize(1<<30), 1<<31)
}

// Ensure a closed database returns an error while running a transaction block
func TestDBRWTransactionBlockWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		err := db.Update(func(txn *RWTransaction) error {
			txn.CreateBucket("widgets")
			return nil
		})
		assert.Equal(t, err, ErrDatabaseNotOpen)
	})
}

// Ensure a closed database returns an error while running a transaction block
func TestDBTransactionBlockWhileClosed(t *testing.T) {
	withDB(func(db *DB, path string) {
		err := db.View(func(txn *Transaction) error {
			txn.Bucket("widgets")
			return nil
		})
		assert.Equal(t, err, ErrDatabaseNotOpen)
	})
}

// withDB executes a function with a database reference.
func withDB(fn func(*DB, string)) {
	f, _ := os.CreateTemp("", "myboltdb-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.RemoveAll(path)

	var db DB
	fn(&db, path)
}

// withMockDB executes a function with a database reference and a mock filesystem.
func withMockDB(fn func(*DB, *mockos, *mocksyscall, string)) {
	os, syscall := &mockos{}, &mocksyscall{}
	var db DB
	db.os = os
	db.syscall = syscall
	fn(&db, os, syscall, "/mock/db")
}

// withOpenDB executes a function with an already opened database.
func withOpenDB(fn func(*DB, string)) {
	withDB(func(db *DB, path string) {
		if err := db.Open(path, 0666); err != nil {
			panic("cannot open db: " + err.Error())
		}
		defer db.Close()
		fn(db, path)
	})
}
