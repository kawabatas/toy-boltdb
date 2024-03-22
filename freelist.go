package toyboltdb

import "unsafe"

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
