package ptable

// Iter ...
type Iter struct {
}

// SeekGE moves the iterator to the first block containing keys greater than or
// equal to the given key.
func (i *Iter) SeekGE(key []byte) {
}

// SeekLT moves the iterator to the last block containing keys less than the
// given key.
func (i *Iter) SeekLT(key []byte) {
}

// First moves the iterator to the first block in the table.
func (i *Iter) First() {
}

// Last moves the iterator to the last block in the table.
func (i *Iter) Last() {
}

// Next moves the iterator to the next block in the table.
func (i *Iter) Next() {
}

// Prev moves the iterator to the previous block in the table.
func (i *Iter) Prev() {
}

// Valid returns true if the iterator is positioned at a valid block and false
// otherwise.
func (i *Iter) Valid() bool {
	return false
}

// Block returns the block the iterator is currently pointed out.
func (i *Iter) Block() *blockReader {
	return nil
}

// Reader ...
type Reader struct {
}

// NewIter ...
func (r *Reader) NewIter() *Iter {
	return nil
}
