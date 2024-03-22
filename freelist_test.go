package toyboltdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that a page is added to a transaction's freelist.
func TestFreelistFree(t *testing.T) {
	f := &freelist{pendingPageIDMap: make(map[txID][]pageID)}
	f.free(100, &page{id: 12})
	assert.Equal(t, f.pendingPageIDMap[100], []pageID{12})
}

// Ensure that a page and its overflow is added to a transaction's freelist.
func TestFreelistFreeOverflow(t *testing.T) {
	f := &freelist{pendingPageIDMap: make(map[txID][]pageID)}
	f.free(100, &page{id: 12, overflow: 3})
	assert.Equal(t, f.pendingPageIDMap[100], []pageID{12, 13, 14, 15})
}

// Ensure that a freelist can find contiguous blocks of pages.
func TestFreelistAllocate(t *testing.T) {
	f := &freelist{pageIDs: []pageID{18, 13, 12, 9, 7, 6, 5, 4, 3}}
	assert.Equal(t, f.allocate(2), pageID(12)) // 13,12
	assert.Equal(t, f.allocate(1), pageID(18)) // 18
	assert.Equal(t, f.allocate(3), pageID(5))  // 7,6,5
	assert.Equal(t, f.allocate(3), pageID(0))
	assert.Equal(t, f.allocate(2), pageID(3)) // 4,3
	assert.Equal(t, f.allocate(1), pageID(9)) // 9
	assert.Equal(t, f.allocate(0), pageID(0))
	assert.Equal(t, f.pageIDs, []pageID{})
}
