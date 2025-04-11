package virtual

import "github.com/cockroachdb/pebble/internal/base"

// VirtualReaderParams are the parameters necessary for a reader to read virtual sstables.
type VirtualReaderParams struct {
	Lower   base.InternalKey
	Upper   base.InternalKey
	FileNum base.FileNum
}

// Constrain bounds will narrow the start, end bounds if they do not fit within
// the virtual sstable. The function will return if the new end key is
// inclusive.
func (v *VirtualReaderParams) ConstrainBounds(
	start, end []byte, endInclusive bool, compare func([]byte, []byte) int,
) (lastKeyInclusive bool, first []byte, last []byte) {
	first = start
	if start == nil || compare(start, v.Lower.UserKey) < 0 {
		first = v.Lower.UserKey
	}

	// Note that we assume that start, end has some overlap with the virtual
	// sstable bounds.
	last = v.Upper.UserKey
	lastKeyInclusive = !v.Upper.IsExclusiveSentinel()
	if end != nil {
		cmp := compare(end, v.Upper.UserKey)
		switch {
		case cmp == 0:
			lastKeyInclusive = !v.Upper.IsExclusiveSentinel() && endInclusive
			last = v.Upper.UserKey
		case cmp > 0:
			lastKeyInclusive = !v.Upper.IsExclusiveSentinel()
			last = v.Upper.UserKey
		default:
			lastKeyInclusive = endInclusive
			last = end
		}
	}
	// TODO(bananabrick): What if someone passes in bounds completely outside of
	// virtual sstable bounds?
	return lastKeyInclusive, first, last
}
