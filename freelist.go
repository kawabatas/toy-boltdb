package toyboltdb

import (
	"fmt"
	"unsafe"
)

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
//
// freelist manages used and unused pages.
//
// A freelist has many pages.
type freelist struct {
	pageIDs          []pageID
	pendingPageIDMap map[txID][]pageID
}

// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
//
// See test cases
func (f *freelist) allocate(n int) pageID {
	var count int
	var previd pageID
	for i, id := range f.pageIDs {
		// Reset count if this is not contiguous.
		if previd == 0 || previd-id != 1 {
			count = 1
		}

		// If we found a contiguous block then remove it and return it.
		if count == n {
			f.pageIDs = append(f.pageIDs[:i-(n-1)], f.pageIDs[i+1:]...)
			if id <= 1 {
				panic(fmt.Sprintf("assertion failed: cannot allocate page 0 or 1: %d", id))
			}
			return id
		}

		previd = id
		count++
	}
	return 0
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txID txID) {
	// TODO
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *page) {
	ids := ((*[maxAllocSize]pageID)(unsafe.Pointer(&p.ptr)))[0:p.count]
	f.pageIDs = make([]pageID, len(ids))
	copy(f.pageIDs, ids)
}
