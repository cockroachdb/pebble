// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base // import "github.com/cockroachdb/pebble/internal/base"

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/redact"
)

// SeqNum is a sequence number defining precedence among identical keys. A key
// with a higher sequence number takes precedence over a key with an equal user
// key of a lower sequence number. Sequence numbers are stored durably within
// the internal key "trailer" as a 7-byte (uint56) uint, and the maximum
// sequence number is 2^56-1. As keys are committed to the database, they're
// assigned increasing sequence numbers. Readers use sequence numbers to read a
// consistent database state, ignoring keys with sequence numbers larger than
// the readers' "visible sequence number."
//
// The database maintains an invariant that no two point keys with equal user
// keys may have equal sequence numbers. Keys with differing user keys may have
// equal sequence numbers. A point key and a range deletion or range key that
// include that point key can have equal sequence numbers - in that case, the
// range key does not apply to the point key. A key's sequence number may be
// changed to zero during compactions when it can be proven that no identical
// keys with lower sequence numbers exist.
type SeqNum uint64

const (
	// SeqNumZero is the zero sequence number, set by compactions if they can
	// guarantee there are no keys underneath an internal key.
	SeqNumZero SeqNum = 0
	// SeqNumStart is the first sequence number assigned to a key. Sequence
	// numbers 1-9 are reserved for potential future use.
	SeqNumStart SeqNum = 10
	// SeqNumMax is the largest valid sequence number.
	SeqNumMax SeqNum = 1<<56 - 1
	// SeqNumBatchBit is set on batch sequence numbers which prevents those
	// entries from being excluded from iteration.
	SeqNumBatchBit SeqNum = 1 << 55
)

func (s SeqNum) String() string {
	if s == SeqNumMax {
		return "inf"
	}
	var batch string
	if s&SeqNumBatchBit != 0 {
		batch = "b"
		s &^= SeqNumBatchBit
	}
	return fmt.Sprintf("%s%d", batch, s)
}

// SafeFormat implements redact.SafeFormatter.
func (s SeqNum) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(redact.SafeString(s.String()))
}

// InternalKeyKind enumerates the kind of key: a deletion tombstone, a set
// value, a merged value, etc.
type InternalKeyKind uint8

// These constants are part of the file format, and should not be changed.
const (
	InternalKeyKindDelete  InternalKeyKind = 0
	InternalKeyKindSet     InternalKeyKind = 1
	InternalKeyKindMerge   InternalKeyKind = 2
	InternalKeyKindLogData InternalKeyKind = 3
	//InternalKeyKindColumnFamilyDeletion     InternalKeyKind = 4
	//InternalKeyKindColumnFamilyValue        InternalKeyKind = 5
	//InternalKeyKindColumnFamilyMerge        InternalKeyKind = 6

	// InternalKeyKindSingleDelete (SINGLEDEL) is a performance optimization
	// solely for compactions (to reduce write amp and space amp). Readers other
	// than compactions should treat SINGLEDEL as equivalent to a DEL.
	// Historically, it was simpler for readers other than compactions to treat
	// SINGLEDEL as equivalent to DEL, but as of the introduction of
	// InternalKeyKindSSTableInternalObsoleteBit, this is also necessary for
	// correctness.
	InternalKeyKindSingleDelete InternalKeyKind = 7
	//InternalKeyKindColumnFamilySingleDelete InternalKeyKind = 8
	//InternalKeyKindBeginPrepareXID          InternalKeyKind = 9
	//InternalKeyKindEndPrepareXID            InternalKeyKind = 10
	//InternalKeyKindCommitXID                InternalKeyKind = 11
	//InternalKeyKindRollbackXID              InternalKeyKind = 12
	//InternalKeyKindNoop                     InternalKeyKind = 13
	//InternalKeyKindColumnFamilyRangeDelete  InternalKeyKind = 14
	InternalKeyKindRangeDelete InternalKeyKind = 15
	//InternalKeyKindColumnFamilyBlobIndex    InternalKeyKind = 16
	//InternalKeyKindBlobIndex                InternalKeyKind = 17

	// InternalKeyKindSeparator is a key used for separator / successor keys
	// written to sstable block indexes.
	//
	// NOTE: the RocksDB value has been repurposed. This was done to ensure that
	// keys written to block indexes with value "17" (when 17 happened to be the
	// max value, and InternalKeyKindMax was therefore set to 17), remain stable
	// when new key kinds are supported in Pebble.
	InternalKeyKindSeparator InternalKeyKind = 17

	// InternalKeyKindSetWithDelete keys are SET keys that have met with a
	// DELETE or SINGLEDEL key in a prior compaction. This key kind is
	// specific to Pebble. See
	// https://github.com/cockroachdb/pebble/issues/1255.
	InternalKeyKindSetWithDelete InternalKeyKind = 18

	// InternalKeyKindRangeKeyDelete removes all range keys within a key range.
	// See the internal/rangekey package for more details.
	InternalKeyKindRangeKeyDelete InternalKeyKind = 19
	// InternalKeyKindRangeKeySet and InternalKeyKindRangeUnset represent
	// keys that set and unset values associated with ranges of key
	// space. See the internal/rangekey package for more details.
	InternalKeyKindRangeKeyUnset InternalKeyKind = 20
	InternalKeyKindRangeKeySet   InternalKeyKind = 21

	InternalKeyKindRangeKeyMin InternalKeyKind = InternalKeyKindRangeKeyDelete
	InternalKeyKindRangeKeyMax InternalKeyKind = InternalKeyKindRangeKeySet

	// InternalKeyKindIngestSST is used to distinguish a batch that corresponds to
	// the WAL entry for ingested sstables that are added to the flushable
	// queue. This InternalKeyKind cannot appear amongst other key kinds in a
	// batch (with the exception of alongside InternalKeyKindExcise), or in an sstable.
	InternalKeyKindIngestSST InternalKeyKind = 22

	// InternalKeyKindDeleteSized keys behave identically to
	// InternalKeyKindDelete keys, except that they hold an associated uint64
	// value indicating the (len(key)+len(value)) of the shadowed entry the
	// tombstone is expected to delete. This value is used to inform compaction
	// heuristics, but is not required to be accurate for correctness.
	InternalKeyKindDeleteSized InternalKeyKind = 23

	// InternalKeyKindExcise is used to persist the Excise part of an IngestAndExcise
	// to a WAL. An Excise is similar to a RangeDel+RangeKeyDel combined, in that it
	// deletes all point and range keys in a given key range while also immediately
	// truncating sstables to exclude this key span. This InternalKeyKind cannot
	// appear amongst other key kinds in a batch (with the exception of alongside
	// InternalKeyKindIngestSST), or in an sstable.
	InternalKeyKindExcise InternalKeyKind = 24

	// This maximum value isn't part of the file format. Future extensions may
	// increase this value.
	//
	// When constructing an internal key to pass to DB.Seek{GE,LE},
	// internalKeyComparer sorts decreasing by kind (after sorting increasing by
	// user key and decreasing by sequence number). Thus, use InternalKeyKindMax,
	// which sorts 'less than or equal to' any other valid internalKeyKind, when
	// searching for any kind of internal key formed by a certain user key and
	// seqNum.
	InternalKeyKindMax InternalKeyKind = 24

	// InternalKeyKindMaxForSSTable is the largest valid key kind that can exist
	// in an SSTable. This should usually equal InternalKeyKindMax, except
	// if the current InternalKeyKindMax is a kind that is never added to an
	// SSTable or memtable (eg. InternalKeyKindExcise).
	InternalKeyKindMaxForSSTable InternalKeyKind = InternalKeyKindDeleteSized

	// Internal to the sstable format. Not exposed by any sstable iterator.
	// Declared here to prevent definition of valid key kinds that set this bit.
	InternalKeyKindSSTableInternalObsoleteBit  InternalKeyKind = 64
	InternalKeyKindSSTableInternalObsoleteMask InternalKeyKind = 191

	// InternalKeyZeroSeqnumMaxTrailer is the largest trailer with a
	// zero sequence number.
	InternalKeyZeroSeqnumMaxTrailer InternalKeyTrailer = 255

	// A marker for an invalid key.
	InternalKeyKindInvalid InternalKeyKind = InternalKeyKindSSTableInternalObsoleteMask

	// InternalKeyRangeDeleteSentinel is the marker for a range delete sentinel
	// key. This sequence number and kind are used for the upper stable boundary
	// when a range deletion tombstone is the largest key in an sstable. This is
	// necessary because sstable boundaries are inclusive, while the end key of a
	// range deletion tombstone is exclusive.
	InternalKeyRangeDeleteSentinel = (InternalKeyTrailer(SeqNumMax) << 8) | InternalKeyTrailer(InternalKeyKindRangeDelete)

	// InternalKeyBoundaryRangeKey is the marker for a range key boundary. This
	// sequence number and kind are used during interleaved range key and point
	// iteration to allow an iterator to stop at range key start keys where
	// there exists no point key.
	InternalKeyBoundaryRangeKey = (InternalKeyTrailer(SeqNumMax) << 8) | InternalKeyTrailer(InternalKeyKindRangeKeySet)
)

// Assert InternalKeyKindSSTableInternalObsoleteBit > InternalKeyKindMax
const _ = uint(InternalKeyKindSSTableInternalObsoleteBit - InternalKeyKindMax - 1)

var internalKeyKindNames = []string{
	InternalKeyKindDelete:         "DEL",
	InternalKeyKindSet:            "SET",
	InternalKeyKindMerge:          "MERGE",
	InternalKeyKindLogData:        "LOGDATA",
	InternalKeyKindSingleDelete:   "SINGLEDEL",
	InternalKeyKindRangeDelete:    "RANGEDEL",
	InternalKeyKindSeparator:      "SEPARATOR",
	InternalKeyKindSetWithDelete:  "SETWITHDEL",
	InternalKeyKindRangeKeySet:    "RANGEKEYSET",
	InternalKeyKindRangeKeyUnset:  "RANGEKEYUNSET",
	InternalKeyKindRangeKeyDelete: "RANGEKEYDEL",
	InternalKeyKindIngestSST:      "INGESTSST",
	InternalKeyKindDeleteSized:    "DELSIZED",
	InternalKeyKindExcise:         "EXCISE",
	InternalKeyKindInvalid:        "INVALID",
}

func (k InternalKeyKind) String() string {
	if int(k) < len(internalKeyKindNames) {
		return internalKeyKindNames[k]
	}
	return fmt.Sprintf("UNKNOWN:%d", k)
}

// SafeFormat implements redact.SafeFormatter.
func (k InternalKeyKind) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(redact.SafeString(k.String()))
}

// InternalKeyTrailer encodes a SeqNum and an InternalKeyKind.
type InternalKeyTrailer uint64

// MakeTrailer constructs an internal key trailer from the specified sequence
// number and kind.
func MakeTrailer(seqNum SeqNum, kind InternalKeyKind) InternalKeyTrailer {
	return (InternalKeyTrailer(seqNum) << 8) | InternalKeyTrailer(kind)
}

// String imlements the fmt.Stringer interface.
func (t InternalKeyTrailer) String() string {
	return fmt.Sprintf("%s,%s", SeqNum(t>>8), InternalKeyKind(t&0xff))
}

// SeqNum returns the sequence number component of the trailer.
func (t InternalKeyTrailer) SeqNum() SeqNum {
	return SeqNum(t >> 8)
}

// Kind returns the key kind component of the trailer.
func (t InternalKeyTrailer) Kind() InternalKeyKind {
	return InternalKeyKind(t & 0xff)
}

// IsExclusiveSentinel returns true if the trailer is a sentinel for an
// exclusive boundary.
func (t InternalKeyTrailer) IsExclusiveSentinel() bool {
	return t.SeqNum() == SeqNumMax
}

// InternalKey is a key used for the in-memory and on-disk partial DBs that
// make up a pebble DB.
//
// It consists of the user key (as given by the code that uses package pebble)
// followed by 8-bytes of metadata:
//   - 1 byte for the type of internal key: delete or set,
//   - 7 bytes for a uint56 sequence number, in little-endian format.
type InternalKey struct {
	UserKey []byte
	Trailer InternalKeyTrailer
}

// InvalidInternalKey is an invalid internal key for which Valid() will return
// false.
var InvalidInternalKey = MakeInternalKey(nil, SeqNumZero, InternalKeyKindInvalid)

// MakeInternalKey constructs an internal key from a specified user key,
// sequence number and kind.
func MakeInternalKey(userKey []byte, seqNum SeqNum, kind InternalKeyKind) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: MakeTrailer(seqNum, kind),
	}
}

// MakeSearchKey constructs an internal key that is appropriate for searching
// for a the specified user key. The search key contain the maximal sequence
// number and kind ensuring that it sorts before any other internal keys for
// the same user key.
func MakeSearchKey(userKey []byte) InternalKey {
	return MakeInternalKey(userKey, SeqNumMax, InternalKeyKindMax)
}

// MakeRangeDeleteSentinelKey constructs an internal key that is a range
// deletion sentinel key, used as the upper boundary for an sstable when a
// range deletion is the largest key in an sstable.
func MakeRangeDeleteSentinelKey(userKey []byte) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: InternalKeyRangeDeleteSentinel,
	}
}

// MakeExclusiveSentinelKey constructs an internal key that is an
// exclusive sentinel key, used as the upper boundary for an sstable
// when a ranged key is the largest key in an sstable.
func MakeExclusiveSentinelKey(kind InternalKeyKind, userKey []byte) InternalKey {
	return MakeInternalKey(userKey, SeqNumMax, kind)
}

var kindsMap = map[string]InternalKeyKind{
	"DEL":           InternalKeyKindDelete,
	"SINGLEDEL":     InternalKeyKindSingleDelete,
	"RANGEDEL":      InternalKeyKindRangeDelete,
	"LOGDATA":       InternalKeyKindLogData,
	"SET":           InternalKeyKindSet,
	"MERGE":         InternalKeyKindMerge,
	"INVALID":       InternalKeyKindInvalid,
	"SEPARATOR":     InternalKeyKindSeparator,
	"SETWITHDEL":    InternalKeyKindSetWithDelete,
	"RANGEKEYSET":   InternalKeyKindRangeKeySet,
	"RANGEKEYUNSET": InternalKeyKindRangeKeyUnset,
	"RANGEKEYDEL":   InternalKeyKindRangeKeyDelete,
	"INGESTSST":     InternalKeyKindIngestSST,
	"DELSIZED":      InternalKeyKindDeleteSized,
	"EXCISE":        InternalKeyKindExcise,
}

// ParseSeqNum parses the string representation of a sequence number.
// "inf" is supported as the maximum sequence number (mainly used for exclusive
// end keys).
func ParseSeqNum(s string) SeqNum {
	if s == "inf" {
		return SeqNumMax
	}
	batch := s[0] == 'b'
	if batch {
		s = s[1:]
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("error parsing %q as seqnum: %s", s, err))
	}
	seqNum := SeqNum(n)
	if batch {
		seqNum |= SeqNumBatchBit
	}
	return seqNum
}

// ParseKind parses the string representation of an internal key kind.
func ParseKind(s string) InternalKeyKind {
	kind, ok := kindsMap[s]
	if !ok {
		panic(fmt.Sprintf("unknown kind: %q", s))
	}
	return kind
}

// InternalTrailerLen is the number of bytes used to encode InternalKey.Trailer.
const InternalTrailerLen = 8

// DecodeInternalKey decodes an encoded internal key. See InternalKey.Encode().
func DecodeInternalKey(encodedKey []byte) InternalKey {
	n := len(encodedKey) - InternalTrailerLen
	var trailer InternalKeyTrailer
	if n >= 0 {
		trailer = InternalKeyTrailer(binary.LittleEndian.Uint64(encodedKey[n:]))
		encodedKey = encodedKey[:n:n]
	} else {
		trailer = InternalKeyTrailer(InternalKeyKindInvalid)
		encodedKey = nil
	}
	return InternalKey{
		UserKey: encodedKey,
		Trailer: trailer,
	}
}

// InternalCompare compares two internal keys using the specified comparison
// function. For equal user keys, internal keys compare in descending sequence
// number order. For equal user keys and sequence numbers, internal keys
// compare in descending kind order (this may happen in practice among range
// keys).
func InternalCompare(userCmp Compare, a, b InternalKey) int {
	if x := userCmp(a.UserKey, b.UserKey); x != 0 {
		return x
	}
	// Reverse order for trailer comparison.
	return cmp.Compare(b.Trailer, a.Trailer)
}

// Encode encodes the receiver into the buffer. The buffer must be large enough
// to hold the encoded data. See InternalKey.Size().
func (k InternalKey) Encode(buf []byte) {
	i := copy(buf, k.UserKey)
	binary.LittleEndian.PutUint64(buf[i:], uint64(k.Trailer))
}

// EncodeTrailer returns the trailer encoded to an 8-byte array.
func (k InternalKey) EncodeTrailer() [8]byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(k.Trailer))
	return buf
}

// Separator returns a separator key such that k <= x && x < other, where less
// than is consistent with the Compare function. The buf parameter may be used
// to store the returned InternalKey.UserKey, though it is valid to pass a
// nil. See the Separator type for details on separator keys.
func (k InternalKey) Separator(
	cmp Compare, sep Separator, buf []byte, other InternalKey,
) InternalKey {
	if invariants.Enabled && (len(k.UserKey) == 0 || len(other.UserKey) == 0) {
		panic(errors.AssertionFailedf("empty keys passed to Separator: %s, %s", k, other))
	}
	buf = sep(buf, k.UserKey, other.UserKey)
	if len(buf) <= len(k.UserKey) && cmp(k.UserKey, buf) < 0 {
		// The separator user key is physically shorter than k.UserKey (if it is
		// longer, we'll continue to use "k"), but logically after. Tack on the max
		// sequence number to the shortened user key. Note that we could tack on
		// any sequence number and kind here to create a valid separator key. We
		// use the max sequence number to match the behavior of LevelDB and
		// RocksDB.
		return MakeInternalKey(buf, SeqNumMax, InternalKeyKindSeparator)
	}
	return k
}

// Successor returns a successor key such that k <= x. A simple implementation
// may return k unchanged. The buf parameter may be used to store the returned
// InternalKey.UserKey, though it is valid to pass a nil.
func (k InternalKey) Successor(cmp Compare, succ Successor, buf []byte) InternalKey {
	buf = succ(buf, k.UserKey)
	if (len(k.UserKey) == 0 || len(buf) <= len(k.UserKey)) && cmp(k.UserKey, buf) < 0 {
		// The successor user key is physically shorter that k.UserKey (if it is
		// longer, we'll continue to use "k"), but logically after. Tack on the max
		// sequence number to the shortened user key. Note that we could tack on
		// any sequence number and kind here to create a valid separator key. We
		// use the max sequence number to match the behavior of LevelDB and
		// RocksDB.
		return MakeInternalKey(buf, SeqNumMax, InternalKeyKindSeparator)
	}
	return k
}

// Size returns the encoded size of the key.
func (k InternalKey) Size() int {
	return len(k.UserKey) + 8
}

// SetSeqNum sets the sequence number component of the key.
func (k *InternalKey) SetSeqNum(seqNum SeqNum) {
	k.Trailer = (InternalKeyTrailer(seqNum) << 8) | (k.Trailer & 0xff)
}

// SeqNum returns the sequence number component of the key.
func (k InternalKey) SeqNum() SeqNum {
	return SeqNum(k.Trailer >> 8)
}

// IsUpperBoundFor returns true if a range ending in k contains the userKey:
// either userKey < k.UserKey or they are equal and k is not an exclusive
// sentinel.
func (k InternalKey) IsUpperBoundFor(cmp Compare, userKey []byte) bool {
	c := cmp(userKey, k.UserKey)
	return c < 0 || (c == 0 && !k.IsExclusiveSentinel())
}

// Visible returns true if the key is visible at the specified snapshot
// sequence number.
func (k InternalKey) Visible(snapshot, batchSnapshot SeqNum) bool {
	return Visible(k.SeqNum(), snapshot, batchSnapshot)
}

// Visible returns true if a key with the provided sequence number is visible at
// the specified snapshot sequence numbers.
func Visible(seqNum SeqNum, snapshot, batchSnapshot SeqNum) bool {
	// There are two snapshot sequence numbers, one for committed keys and one
	// for batch keys. If a seqNum is less than `snapshot`, then seqNum
	// corresponds to a committed key that is visible. If seqNum has its batch
	// bit set, then seqNum corresponds to an uncommitted batch key. Its
	// visible if its snapshot is less than batchSnapshot.
	//
	// There's one complication. The maximal sequence number
	// (`InternalKeySeqNumMax`) is used across Pebble for exclusive sentinel
	// keys and other purposes. The maximal sequence number has its batch bit
	// set, but it can never be < `batchSnapshot`, since there is no expressible
	// larger snapshot. We dictate that the maximal sequence number is always
	// visible.
	return seqNum < snapshot ||
		((seqNum&SeqNumBatchBit) != 0 && seqNum < batchSnapshot) ||
		seqNum == SeqNumMax
}

// SetKind sets the kind component of the key.
func (k *InternalKey) SetKind(kind InternalKeyKind) {
	k.Trailer = (k.Trailer &^ 0xff) | InternalKeyTrailer(kind)
}

// Kind returns the kind component of the key.
func (k InternalKey) Kind() InternalKeyKind {
	return k.Trailer.Kind()
}

// Valid returns true if the key has a valid kind.
func (k InternalKey) Valid() bool {
	return k.Kind() <= InternalKeyKindMax
}

// Clone clones the storage for the UserKey component of the key.
func (k InternalKey) Clone() InternalKey {
	if len(k.UserKey) == 0 {
		return k
	}
	return InternalKey{
		UserKey: append([]byte(nil), k.UserKey...),
		Trailer: k.Trailer,
	}
}

// CopyFrom converts this InternalKey into a clone of the passed-in InternalKey,
// reusing any space already used for the current UserKey.
func (k *InternalKey) CopyFrom(k2 InternalKey) {
	k.UserKey = append(k.UserKey[:0], k2.UserKey...)
	k.Trailer = k2.Trailer
}

// String returns a string representation of the key.
func (k InternalKey) String() string {
	return fmt.Sprintf("%s#%s,%s", FormatBytes(k.UserKey), k.SeqNum(), k.Kind())
}

// Pretty returns a formatter for the key.
func (k InternalKey) Pretty(f FormatKey) fmt.Formatter {
	return prettyInternalKey{k, f}
}

// IsExclusiveSentinel returns whether this internal key excludes point keys
// with the same user key if used as an end boundary. See the comment on
// InternalKeyRangeDeletionSentinel.
func (k InternalKey) IsExclusiveSentinel() bool {
	if k.SeqNum() != SeqNumMax {
		return false
	}
	switch kind := k.Kind(); kind {
	case InternalKeyKindRangeDelete, InternalKeyKindRangeKeyDelete,
		InternalKeyKindRangeKeyUnset, InternalKeyKindRangeKeySet:
		return true
	default:
		return false
	}
}

type prettyInternalKey struct {
	InternalKey
	formatKey FormatKey
}

func (k prettyInternalKey) Format(s fmt.State, c rune) {
	fmt.Fprintf(s, "%s#%s,%s", k.formatKey(k.UserKey), k.SeqNum(), k.Kind())
}

// ParseInternalKey parses the string representation of an internal key. The
// format is <user-key>#<seq-num>,<kind>. The older format
// <user-key>.<kind>.<seq-num> is also supported (for now).
//
// If the seq-num starts with a "b" it is marked as a batch-seq-num (i.e. the
// SeqNumBatchBit bit is set).
func ParseInternalKey(s string) InternalKey {
	if !strings.Contains(s, "#") {
		// Parse the old format: <user-key>.<kind>.<seq-num>
		// TODO(radu): get rid of this.
		x := strings.Split(s, ".")
		if len(x) != 3 {
			panic(fmt.Sprintf("invalid internal key %q", s))
		}
		ukey := x[0]
		kind, ok := kindsMap[x[1]]
		if !ok {
			panic(fmt.Sprintf("unknown kind: %q", x[1]))
		}
		seqNum := ParseSeqNum(x[2])
		return MakeInternalKey([]byte(ukey), seqNum, kind)
	}
	x := strings.FieldsFunc(s, func(c rune) bool { return c == '#' || c == ',' })
	if len(x) != 3 {
		panic(fmt.Sprintf("invalid key internal %q", s))
	}
	userKey := []byte(x[0])
	seqNum := ParseSeqNum(x[1])
	kind, ok := kindsMap[x[2]]
	if !ok {
		panic(fmt.Sprintf("unknown kind: %q", x[2]))
	}
	return MakeInternalKey(userKey, seqNum, kind)
}

// ParseInternalKeyRange parses a string of the form:
//
//	[<user-key>#<seq-num>,<kind>-<user-key>#<seq-num>,<kind>]
func ParseInternalKeyRange(s string) (start, end InternalKey) {
	s, ok1 := strings.CutPrefix(s, "[")
	s, ok2 := strings.CutSuffix(s, "]")
	x := strings.Split(s, "-")
	if !ok1 || !ok2 || len(x) != 2 {
		panic(fmt.Sprintf("invalid key range %q", s))
	}
	return ParseInternalKey(x[0]), ParseInternalKey(x[1])
}

// MakeInternalKV constructs an InternalKV with the provided internal key and
// value. The value is encoded in-place.
func MakeInternalKV(k InternalKey, v []byte) InternalKV {
	return InternalKV{
		K: k,
		V: MakeInPlaceValue(v),
	}
}

// InternalKV represents a single internal key-value pair.
type InternalKV struct {
	K InternalKey
	V InternalValue
	M KVMeta
}

// The zero values of TieringSpanID and TieringAttribute are reserved to
// represent absence of these fields. A non-zero TieringSpanID with a zero
// TieringAttribute represents an error in extraction.

type TieringSpanID uint64
type TieringAttribute uint64

// TieringMeta can only be populated for SET or SETWITHDEL key-value pairs.
type TieringMeta struct {
	SpanID TieringSpanID
	// Attribute is a user-specified attribute for the key-value pair.
	//
	// TODO(sumeer): For CockroachDB decide on units for this attribute, which
	// will be a timestamp, since unix nanos is unnecessarily large.
	// log2(24*365*100) = 19.74, i.e., number of hours in 100 years fits in 3
	// bytes.
	Attribute TieringAttribute
}

// KVMeta is optional information that is known about the InternalKV. For now,
// this is available only for KVs read from an sstable. Placing this in
// InternalKV allows this to be exposed via the InternalIterator interface.
//
// TODO(sumeer): we are doing this only because we need this information
// during a sstable compaction. It is not needed for normal iteration, so we
// shouldn't be adding additional overhead for the common path. We should
// specialize levelIter and the sstable iterators to have FirstWithMeta and
// NextWithMeta methods that can be used for the limited use case of
// compaction.Iter.
type KVMeta struct {
	Tiering TieringMeta
}

// Kind returns the KV's internal key kind.
func (kv *InternalKV) Kind() InternalKeyKind {
	return kv.K.Kind()
}

// SeqNum returns the KV's internal key sequence number.
func (kv *InternalKV) SeqNum() SeqNum {
	return kv.K.SeqNum()
}

// InPlaceValue returns the KV's in-place value.
func (kv *InternalKV) InPlaceValue() []byte {
	return kv.V.InPlaceValue()
}

// LazyValue returns a LazyValue containing the KV's value.
func (kv *InternalKV) LazyValue() LazyValue {
	return kv.V.LazyValue()
}

// Value returns the KV's underlying value.
func (kv *InternalKV) Value(buf []byte) (val []byte, callerOwned bool, err error) {
	return kv.V.Value(buf)
}

// Visible returns true if the key is visible at the specified snapshot
// sequence number.
func (kv *InternalKV) Visible(snapshot, batchSnapshot SeqNum) bool {
	return Visible(kv.K.SeqNum(), snapshot, batchSnapshot)
}

// IsExclusiveSentinel returns whether this key excludes point keys
// with the same user key if used as an end boundary. See the comment on
// InternalKeyRangeDeletionSentinel.
func (kv *InternalKV) IsExclusiveSentinel() bool {
	return kv.K.IsExclusiveSentinel()
}

// AtomicSeqNum is an atomic SeqNum.
type AtomicSeqNum struct {
	value atomic.Uint64
}

// Load atomically loads and returns the stored SeqNum.
func (asn *AtomicSeqNum) Load() SeqNum {
	return SeqNum(asn.value.Load())
}

// Store atomically stores s.
func (asn *AtomicSeqNum) Store(s SeqNum) {
	asn.value.Store(uint64(s))
}

// Add atomically adds delta to asn and returns the new value.
func (asn *AtomicSeqNum) Add(delta SeqNum) SeqNum {
	return SeqNum(asn.value.Add(uint64(delta)))
}

// CompareAndSwap executes the compare-and-swap operation.
func (asn *AtomicSeqNum) CompareAndSwap(old, new SeqNum) bool {
	return asn.value.CompareAndSwap(uint64(old), uint64(new))
}

// TieringPolicy defines a policy for tiering key-value pairs into warm and
// cold tiers.
type TieringPolicy struct {
	// SpanID is an immutable id for the key span to which this policy applies.
	// The actual span is specified by the SpanPolicy.KeyRange context in which
	// this policy is returned.
	SpanID TieringSpanID
	// ColdTierLTThreshold is the threshold such that 0 < attribute < threshold
	// belongs in the cold tier. For a SpanID, this threshold can change over
	// time, because the typical policy uses the age of data, and (a) the age
	// changes as time advances, (b) the user can change the age threshold that
	// qualifies data for the cold tier.
	ColdTierLTThreshold TieringAttribute
}

// TieringPolicyAndExtractor defines a tiering policy and an extractor for the
// tiering attribute for that policy.
//
// Currently, the only way to retrieve a TieringPolicyAndExtractor is via
// SpanPolicyFunc, by passing a key parameter. The policy is needed by Pebble
// in the following cases:
//
//   - During the execution phase of a flush or a sstable compaction, to do
//     attribute extraction, or to decide which tier a particular row belongs
//     to. Since the key is known, the SpanPolicy can be retrieved with that
//     key. Typically, attribute extraction is done during flushes, and we
//     never re-extract during compactions. However, due to the eventual
//     consistency of the tiering policies, we may need to extract for the
//     first time during a sstable compaction. Note that we cannot extract for
//     the first time when doing a blob file rewrite compaction since the key
//     that determines the policy is not known.
//
//   - During a blob file rewrite compaction. We do not store the key with
//     each value in the blob file, but we store a non-tight key span for the
//     whole blob file. The start key of that span is used to retrieve the
//     first SpanPolicy, and the SpanPolicy.KeyRange.End is used to iterate
//     until we reach the blob file end key. Since the blob file may be have
//     been rewritten in the past (hence the key span is not tight), we may
//     retrieve some unnecessary policies, but we will have all the SpanIDs
//     that could possibly apply to these values and can stash them into a
//     SpanID => TieringPolicy map, for use in this compaction. NB: due to
//     weak consistency, the SpanIDs in this map may be a subset of the
//     SpanIDs in the blob file. For the ones with an unknown policy, we will
//     not change the tier.
//
//   - Before starting a sstable compaction, a decision needs to be made
//     whether to rewrite certain warm and cold blob files referenced in the
//     compaction. This rewrite decision uses the latest tiering policies for
//     all the spanIDs in the inputs of the compactions, and their tiering
//     attribute histograms. It may result in a decision to rewrite a blob
//     file, if it allows for significant movement of data between tiers. In a
//     similar vein, when writing new blob files, a decision needs to be made
//     up front whether there is enough cold data to justify writing a cold
//     blob file (to avoid having tiny files). The same iteration approach
//     mentioned earlier is used.
//
//   - Periodic calls, to learn the latest ColdTierLTThresholds, so that it
//     can initiate explicit rewrites of files that are not being rewritten
//     normally, to move data between tiers. The same iteration approach
//     mentioned earlier is used to iterate over *all* tiering policies.
//
//   - Called when DB.TieringPolicyChange is called, when the aforementioned
//     periodic calls are insufficient. The same iteration approach mentioned
//     earlier is used. to iterate over *all* tiering policies.
//
// There is a concern that iteration over policies in the cases that are not
// using actual keys stored in Pebble will result in unnecessary iteration
// over 100s of policies for CockroachDB tenants that have no ranges on this
// DB. One way to mitigate this is by adding an interface to lookup the
// TieringPolicy by SpanID.
type TieringPolicyAndExtractor struct {
	// Policy returns the tiering policy.
	Policy TieringPolicy
	// ExtractAttribute extracts the tiering attribute from the key-value pair.
	// Once extracted, the attribute can be remembered since it must never
	// change for this key-value pair during the lifetime of the DB.
	//
	// Successful extraction must not return a 0 attribute. Pebble reserves the
	// 0 attribute (with a non-zero SpanID) to represent an extraction error,
	// and stats are maintained for this so that users can enquire about the
	// bytes in the system that have such errors.
	ExtractAttribute func(userKey []byte, value []byte) (TieringAttribute, error)
}

func (tpe TieringPolicyAndExtractor) IsEmpty() bool {
	return tpe.Policy == (TieringPolicy{})
}

type StorageTier uint8

const (
	HotTier StorageTier = iota
	ColdTier
	NumStorageTiers
)

// Tiering and iterators.
//
// We consider the question of correctness when the tiering attribute value is
// used to ignore key-value pairs during iteration. Specifically, consider a
// SQL scan over a SQL index, that corresponds to a single TieringSpanID,
// where the scan is only interested in rows with a tiering attribute value >=
// T.
//
// We first note that we have an existing mechanism to ignore whole blocks of
// key-value pairs during iteration, based on a block property filter.
// Historically, this could expose keys that were deleted (see CockroachDB's
// MVCCIncrementalIterator that still uses a pair of iterators, one with and
// one without a block property filter, to overcome the additional keys
// exposed by the iterator with the block property filter). However, as part
// of supporting range key masking, Pebble's block property filter was
// strengthened to be correct wrt hiding keys that are not visible from a
// Pebble perspective. This correctness depends on the block property
// collector being a function only of the key. See
// https://github.com/cockroachdb/pebble/blob/82fc444f6f79c4a135ef58bf954c2a134e958aed/sstable/block_property.go#L51-L83
// for details.
//
// We now consider the ExtractAttribute function, which is a function of both
// the userKey and value. This is needed since it will be common for the SQL
// column from which the tiering attribute is derived to be part of the value.
// We will also discuss below the case where the tiering attribute is part of
// the key. Along with this we consider two ways of tiering:
//
// - Only-value-cold: Only the value is written to the cold tier in a cold
//   blob file. This is our initial implementation.
//
// - Key-and-value-cold: In addition to cold blob files, this scheme will have
//   cold sstables (say in L7 and L8). This is needed when tiering large SQL
//   secondary indexes where the key is large and the value is small. We have
//   seen such examples in at least one customer workload. We are not yet
//   implementing this, but we want to consider this in the correctness
//   discussion.
//
// Example 1:
//
// The LSM contains k1@t1#seq2 => v2, and k1@t1#seq1 => v1. where seq2 > seq1.
// Since the tiering attribute is a function of (k1, v2) and (k1, v1)
// respectively, it is possible that extract(k1@t1,v2) < T and
// extract(k1@t1,v1) >= T. If we hide k1@t1#seq2 => v2 when iterating (say in
// the sstable iterator), then we can incorrectly expose k1@t1#seq1 => v1,
// even though it is not visible. NB: this example is agnostic to whether we
// are operating with only-value-cold or key-and-value-cold.
//
// Example 2:
//
// One could claim that CockroachDB never reuses the same key k1 (this is not
// actually true, since a txn can abort and another txn can then write to the
// same timestamp, or a txn can write to the same key multiple times). Here we
// show that correctness at the MVCC level is also not guaranteed.
//
// The LSM contains k1@t2#seq2 => v2, and k1@t1#seq1 => v1. where seq2 > seq1,
// and t2 > t1. Both keys are visible from Pebble's perspective. Again, it is
// possible for extract(k1@t2,v2) < T and extract(k1@t1,v1) >= T. If we hide
// k1@t2#seq2 => v2 when iterating, then the MVCC history is incorrect at the
// CockroachDB level.
//
// Solution Part 1:
//
// For TieringSpanIDs where the extraction is a function of both key and
// value, we will do only-value-cold tiering. Additionally, iterators on such
// spans cannot ask for any filtering based on the tiering attribute. For
// efficiency, either the iterator spans must naturally touch only hot data,
// or the iteration must typically stop before reaching cold data.
//
// We have examined the existing customer schemas for tiering and prospective
// queries, and this approach seems sufficient. TODO(sumeer): document the
// analysis in an internal doc.
//
// Solution Part 2:
//
// Applies to TieringSpanIDs for which the extraction is a function of only
// the key prefix (which makes the above examples correct). There are existing
// examples of SQL secondary indexes where the tiering column is part of the
// key. Additionally, we can add the tiering column to the key when the
// secondary index is not UNIQUE. In this case we can do key-and-value-cold
// tiering, and block property filters will be defined over the tiering
// attribute, to be able to skip over cold sstables, and cold sstable blocks.
//
// TODO(sumeer): document the workload analysis in an internal doc.
