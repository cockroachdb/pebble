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
	// queue. This InternalKeyKind cannot appear, amongst other key kinds in a
	// batch, or in an sstable.
	InternalKeyKindIngestSST InternalKeyKind = 22

	// InternalKeyKindDeleteSized keys behave identically to
	// InternalKeyKindDelete keys, except that they hold an associated uint64
	// value indicating the (len(key)+len(value)) of the shadowed entry the
	// tombstone is expected to delete. This value is used to inform compaction
	// heuristics, but is not required to be accurate for correctness.
	InternalKeyKindDeleteSized InternalKeyKind = 23

	// This maximum value isn't part of the file format. Future extensions may
	// increase this value.
	//
	// When constructing an internal key to pass to DB.Seek{GE,LE},
	// internalKeyComparer sorts decreasing by kind (after sorting increasing by
	// user key and decreasing by sequence number). Thus, use InternalKeyKindMax,
	// which sorts 'less than or equal to' any other valid internalKeyKind, when
	// searching for any kind of internal key formed by a certain user key and
	// seqNum.
	InternalKeyKindMax InternalKeyKind = 23

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

// SeqNum returns the sequence number component of the trailer.
func (t InternalKeyTrailer) SeqNum() SeqNum {
	return SeqNum(t >> 8)
}

// Kind returns the key kind component of the trailer.
func (t InternalKeyTrailer) Kind() InternalKeyKind {
	return InternalKeyKind(t & 0xff)
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
}

// ParseInternalKey parses the string representation of an internal key. The
// format is <user-key>.<kind>.<seq-num>. If the seq-num starts with a "b" it
// is marked as a batch-seq-num (i.e. the SeqNumBatchBit bit is set).
func ParseInternalKey(s string) InternalKey {
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
	if len(buf) <= len(k.UserKey) && cmp(k.UserKey, buf) < 0 {
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

// ParsePrettyInternalKey parses the pretty string representation of an
// internal key. The format is <user-key>#<seq-num>,<kind>.
// TODO(radu): do we need both ParseInternalKey and ParsePrettyInternalKey?
func ParsePrettyInternalKey(s string) InternalKey {
	x := strings.FieldsFunc(s, func(c rune) bool { return c == '#' || c == ',' })
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
	return ParsePrettyInternalKey(x[0]), ParsePrettyInternalKey(x[1])
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
	V LazyValue
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

// Value return's the KV's underlying value.
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
