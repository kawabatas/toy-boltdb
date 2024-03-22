// The underlying page and the page's parent pages into memory as "nodes".
// It's like a row, same as pageElement. We use the B+tree, so the "node" exists.
//
// These nodes are where mutations occur during read-write transactions.
// These changes get flushed to disk during commit.
package toyboltdb

// node represents an in-memory, deserialized page.
type node struct {
}
