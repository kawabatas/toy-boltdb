package toyboltdb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	minMmapSize = 1 << 22 // 4MB
	maxMmapStep = 1 << 30 // 1GB
)

const (
	errMsgStat         = "stat error"
	errMsgMeta         = "meta error"
	errMsgFileTooSmall = "file size too small"
	errMsgMmapStat     = "mmap stat error"
)

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {
	os       _os
	syscall  _syscall
	path     string
	file     file
	metafile file
	mmapdata []byte // mmap
	meta0    *meta
	meta1    *meta
	pageSize int
	isOpened bool
	rwtx     *RWTransaction
	txs      []*Transaction
	freelist *freelist

	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
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
	var err error
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Initialize OS/Syscall references.
	// These are overridden by mocks during some tests.
	if db.os == nil {
		db.os = &sysos{}
	}
	if db.syscall == nil {
		db.syscall = &syssyscall{}
	}

	// Exit if the database is currently open.
	if db.isOpened {
		return ErrDatabaseOpen
	}

	// Open data file and separate **sync handler** for metadata writes.
	db.path = path
	if db.file, err = db.os.OpenFile(db.path, os.O_RDWR|os.O_CREATE, mode); err != nil {
		db.close()
		return err
	}
	if db.metafile, err = db.os.OpenFile(db.path, os.O_RDWR|os.O_SYNC, mode); err != nil {
		db.close()
		return err
	}

	// Initialize the database if it doesn't exist.
	if info, err := db.file.Stat(); err != nil {
		return fmt.Errorf("%s: %w", errMsgStat, err)
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		if err := db.init(); err != nil {
			return err
		}
	} else {
		// Read the first meta page to determine the page size.
		var buf [0x1000]byte // QQQ 0x1000 -> 4096 4KiB the default page size?
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			// pageID 0
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				return fmt.Errorf("%s: %w", errMsgMeta, err)
			}
			db.pageSize = int(m.pageSize)
		}
	}

	// Memory map the data file.
	if err := db.mmap(0); err != nil {
		db.close()
		return err
	}

	// Read in the freelist.
	db.freelist = &freelist{pendingPageIDMap: make(map[txID][]pageID)}
	db.freelist.read(db.page(db.meta().freelistPageID))

	// Mark the database as opened and return.
	db.isOpened = true
	return nil
}

// init creates a new database file and initializes its meta pages.
//
// | M(0) | M(1) | F(2) | D(3)        | | | | | |
// |	    |      |      | leaf bucket
//
// and write to our data file
func (db *DB) init() error {
	// Set the page size to the OS page size.
	db.pageSize = db.os.Getpagesize()

	// Create two meta pages on a buffer.
	buf := make([]byte, db.pageSize*4) // M,M,F,D
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pageID(i))
		p.id = pageID(i)
		p.flags = metaPageFlag

		// Initialize the meta page.
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelistPageID = 2
		m.bucketsPageID = 3
		m.pageID = 4
		m.txID = txID(i) // tx 0, tx 1
	}

	// Write an empty freelist at page 3.
	p := db.pageInBuffer(buf[:], pageID(2))
	p.id = pageID(2)
	p.flags = freelistPageFlag
	p.count = 0

	// Write an empty leaf page at page 4.
	p = db.pageInBuffer(buf[:], pageID(3))
	p.id = pageID(3)
	p.flags = bucketsPageFlag
	p.count = 0

	// Write the buffer to our data file.
	if _, err := db.metafile.WriteAt(buf, 0); err != nil {
		return err
	}

	return nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	// Dereference all mmap references before unmapping.
	if db.rwtx != nil {
		db.rwtx.dereference()
	}

	// Unmap existing data before continuing.
	db.munmap()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("%s: %w", errMsgMmapStat, err)
	} else if int(info.Size()) < db.pageSize*2 {
		return errors.New(errMsgFileTooSmall)
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size = db.mmapSize(minsz)

	// mmap() syscall: allocate new memory space to a running process
	// Memory-map the data file as a byte slice.
	if db.mmapdata, err = db.syscall.Mmap(int(db.file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		return err
	}

	// Save references to the meta pages.
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()
	// Validate the meta pages.
	if err := db.meta0.validate(); err != nil {
		return fmt.Errorf("meta0 error: %w", err)
	}
	if err := db.meta1.validate(); err != nil {
		return fmt.Errorf("meta1 error: %w", err)
	}
	return nil
}

// munmap unmaps the data file from memory.
func (db *DB) munmap() {
	if db.mmapdata != nil {
		if err := db.syscall.Munmap(db.mmapdata); err != nil {
			panic("unmap error: " + err.Error())
		}
		db.mmapdata = nil
	}
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 4MB and doubles until it reaches 1GB.
func (db *DB) mmapSize(size int) int {
	if size < minMmapSize {
		return minMmapSize
	} else if size < maxMmapStep {
		size *= 2
	} else {
		size += maxMmapStep
	}

	// Ensure that the mmap size is a multiple of the page size.
	if (size % db.pageSize) != 0 {
		size = ((size / db.pageSize) + 1) * db.pageSize
	}

	return size
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() {
	db.metalock.Lock()
	defer db.metalock.Unlock()
	db.close()
}

func (db *DB) close() {
	db.isOpened = false

	// TODO(benbjohnson): Undo everything in Open().
	db.freelist = nil
	db.path = ""

	db.munmap()
}

// txBegin creates a read-only transaction.
// Multiple read-only transactions can be used concurrently.
//
// IMPORTANT: You must close the transaction after you are finished or else the database will not reclaim old pages.
func (db *DB) txBegin() (*Transaction, error) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.isOpened {
		return nil, ErrDatabaseNotOpen
	}

	// Obtain a read-only lock on the mmap. When the mmap is remapped it will
	// obtain a write lock so all transactions must finish before it can be
	// remapped.
	db.mmaplock.RLock()

	// Create a transaction associated with the database.
	t := &Transaction{}
	t.init(db)

	// Keep track of transaction until it closes.
	db.txs = append(db.txs, t)

	return t, nil
}

// txEnd removes a transaction from the database.
// This is called from Close() on the transaction.
func (db *DB) txEnd(t *Transaction) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Release the read lock on the mmap.
	db.mmaplock.RUnlock()

	// Remove the transaction.
	for i, tx := range db.txs {
		if tx == t {
			db.txs = append(db.txs[:i], db.txs[i+1:]...)
			break
		}
	}
}

// rwtxBegin creates a read/write transaction.
// Only one read/write transaction is allowed at a time.
// You must call Commit() or Rollback() on the transaction to close it.
func (db *DB) rwtxBegin() (*RWTransaction, error) {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.isOpened {
		return nil, ErrDatabaseNotOpen
	}

	// Obtain writer lock. This is released by the RWTransaction when it closes.
	db.rwlock.Lock()

	// Create a transaction associated with the database.
	t := &RWTransaction{nodes: make(map[pageID]*node)}
	t.init(db)
	db.rwtx = t

	// Free any pages associated with closed read-only transactions.
	var minid txID = 0xFFFFFFFFFFFFFFFF
	for _, t := range db.txs {
		if t.meta.txID < minid {
			minid = t.meta.txID
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

// rwtxEnd is called from Commit() or Rollback() on the transaction.
func (db *DB) rwtxEnd() {
	db.rwlock.Unlock()
}

// Update executes a function within the context of a RWTransaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
func (db *DB) Update(fn func(*RWTransaction) error) error {
	t, err := db.rwtxBegin()
	if err != nil {
		return err
	}

	// If an error is returned from the function then rollback and return error.
	if err := fn(t); err != nil {
		t.Rollback()
		return err
	}

	return t.Commit()
}

// View executes a function within the context of a Transaction.
// Any error that is returned from the function is returned from the View() method.
func (db *DB) View(fn func(*Transaction) error) error {
	t, err := db.txBegin()
	if err != nil {
		return err
	}
	defer t.Close()

	// If an error is returned from the function then pass it through.
	return fn(t)
}

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {
	if db.meta0.txID > db.meta1.txID {
		return db.meta0
	}
	return db.meta1
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id pageID) *page {
	return (*page)(unsafe.Pointer(&db.mmapdata[id*pageID(db.pageSize)]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id pageID) *page {
	return (*page)(unsafe.Pointer(&b[id*pageID(db.pageSize)]))
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	buf := make([]byte, count*db.pageSize)
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// Use pages from the freelist **if they are available**.
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.rwtx.meta.pageID
	var minsz = int((p.id+pageID(count))+1) * db.pageSize
	if minsz >= len(db.mmapdata) {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %w", err)
		}
	}

	// Move the page id high water mark.
	db.rwtx.meta.pageID += pageID(count)

	return p, nil
}
