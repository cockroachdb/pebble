package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"unsafe"

	"github.com/petermattis/pebble/db"
)

var propTagMap = make(map[string]reflect.StructField)

var columnFamilyIDField = func() reflect.StructField {
	f, ok := reflect.TypeOf(Properties{}).FieldByName("ColumnFamilyID")
	if !ok {
		panic("Properties.ColumnFamilyID field not found")
	}
	return f
}()

var propOffsetTagMap = make(map[uintptr]db.InternalKey)

func init() {
	t := reflect.TypeOf(Properties{})
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tag := f.Tag.Get("prop"); tag != "" {
			switch f.Type.Kind() {
			case reflect.Bool:
			case reflect.Uint32:
			case reflect.Uint64:
			case reflect.String:
			default:
				panic(fmt.Sprintf("unsupported property field type: %s %s", f.Name, f.Type))
			}
			propTagMap[tag] = f
			propOffsetTagMap[f.Offset] = db.InternalKey{UserKey: []byte(tag)}
		}
	}
}

// Properties TODO(peter)
type Properties struct {
	// ID of column family for this SST file, corresponding to the CF identified
	// by column_family_name.
	ColumnFamilyID uint64 `prop:"rocksdb.column.family.id"`
	// Name of the column family with which this SST file is associated. Empty if
	// the column family is unknown.
	ColumnFamilyName string `prop:"rocksdb.column.family.name"`
	// The name of the comparator used in this table.
	ComparatorName string `prop:"rocksdb.comparator"`
	// The compression algorithm used to compress blocks.
	CompressionName string `prop:"rocksdb.compression"`
	// The time when the SST file was created. Since SST files are immutable,
	// this is equivalent to last modified time.
	CreationTime uint64 `prop:"rocksdb.creation.time"`
	// The total size of all data blocks.
	DataSize uint64 `prop:"rocksdb.data.size"`
	// The name of the filter policy used in this table. Empty if no filter
	// policy is used.
	FilterPolicyName string `prop:"rocksdb.filter.policy"`
	// The size of filter block.
	FilterSize uint64 `prop:"rocksdb.filter.size"`
	// If 0, key is variable length. Otherwise number of bytes for each key.
	FixedKeyLen uint64 `prop:"rocksdb.fixed.key.length"`
	// format version, reserved for backward compatibility.
	FormatVersion uint64 `prop:"rocksdb.format.version"`
	// The global sequence number to use for all entries in the table. Present if
	// the table was created externally and ingested whole.
	GlobalSeqNum uint64 `prop:"rocksdb.external_sst_file.global_seqno"`
	// Whether the index key is user key or an internal key.
	IndexKeyIsUserKey uint64 `prop:"rocksdb.index.key.is.user.key"`
	// Total number of index partitions if kTwoLevelIndexSearch is used.
	IndexPartitions uint64 `prop:"rocksdb.index.partitions"`
	// The size of index block.
	IndexSize uint64 `prop:"rocksdb.index.size"`
	// The index type. TODO(peter): add a more detailed description.
	IndexType uint32 `prop:"rocksdb.block.based.table.index.type"`
	// The name of the merge operator used in this table. Empty if no merge
	// operator is used.
	MergeOperatorName string `prop:"rocksdb.merge.operator"`
	// The number of blocks in this table.
	NumDataBlocks uint64 `prop:"rocksdb.num.data.blocks"`
	// the number of entries in this table.
	NumEntries uint64 `prop:"rocksdb.num.entries"`
	// the number of range deletions in this table.
	NumRangeDeletions uint64 `prop:"rocksdb.num.range-deletions"`
	// Timestamp of the earliest key. 0 if unknown.
	OldestKeyTime uint64 `prop:"rocksdb.oldest.key.time"`
	// The name of the prefix extractor used in this table. Empty if no prefix
	// extractor is used.
	PrefixExtractorName string `prop:"rocksdb.prefix.extractor.name"`
	// If filtering is enabled, was the filter created on the key prefix.
	PrefixFiltering bool `prop:"rocksdb.block.based.table.prefix.filtering"`
	// A comma separated list of names of the property collectors used in this
	// table.
	PropertyCollectorNames string `prop:"rocksdb.property.collectors"`
	// Total raw key size.
	RawKeySize uint64 `prop:"rocksdb.raw.key.size"`
	// Total raw value size.
	RawValueSize uint64 `prop:"rocksdb.raw.value.size"`
	// Size of the top-level index if kTwoLevelIndexSearch is used.
	TopLevelIndexSize uint64 `prop:"rocksdb.top-level.index.size"`
	// User collected properties.
	UserProperties map[string]string
	// The version. TODO(peter): add a more detailed description.
	Version uint64 `prop:"rocksdb.external_sst_file.version"`
	// If filtering is enabled, was the filter created on the whole key.
	WholeKeyFiltering bool `prop:"rocksdb.block.based.table.whole.key.filtering"`
}

func (p *Properties) String() string {
	var buf bytes.Buffer
	v := reflect.ValueOf(*p)
	vt := v.Type()
	for i := 0; i < v.NumField(); i++ {
		ft := vt.Field(i)
		tag := ft.Tag.Get("prop")
		if tag == "" {
			continue
		}
		fmt.Fprintf(&buf, "%s: ", tag)
		f := v.Field(i)
		switch ft.Type.Kind() {
		case reflect.Bool:
			fmt.Fprintf(&buf, "%t\n", f.Bool())
		case reflect.Uint32:
			fmt.Fprintf(&buf, "%d\n", f.Uint())
		case reflect.Uint64:
			u := f.Uint()
			if ft.Offset == columnFamilyIDField.Offset && u == math.MaxInt32 {
				fmt.Fprintf(&buf, "-\n")
			} else {
				fmt.Fprintf(&buf, "%d\n", f.Uint())
			}
		case reflect.String:
			fmt.Fprintf(&buf, "%s\n", f.String())
		default:
			panic("not reached")
		}
	}
	keys := make([]string, 0, len(p.UserProperties))
	for key := range p.UserProperties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s: %s\n", keys, p.UserProperties[key])
	}
	return buf.String()
}

func (p *Properties) load(b block) error {
	i, err := newRawBlockIter(bytes.Compare, b)
	if err != nil {
		return err
	}
	v := reflect.ValueOf(p).Elem()
	for i.First(); i.Valid(); i.Next() {
		tag := i.Key().UserKey
		if f, ok := propTagMap[string(tag)]; ok {
			field := v.FieldByIndex(f.Index)
			switch f.Type.Kind() {
			case reflect.Bool:
				field.SetBool(string(i.Value()) == "1")
			case reflect.Uint32:
				field.SetUint(uint64(binary.LittleEndian.Uint32(i.Value())))
			case reflect.Uint64:
				n, _ := binary.Uvarint(i.Value())
				field.SetUint(n)
			case reflect.String:
				field.SetString(string(i.Value()))
			default:
				panic("not reached")
			}
			continue
		}
		if p.UserProperties == nil {
			p.UserProperties = make(map[string]string)
		}
		p.UserProperties[string(tag)] = string(i.Value())
	}
	return nil
}

func (p *Properties) saveBool(w *rawBlockWriter, offset uintptr, value bool) {
	tag := propOffsetTagMap[offset]
	if value {
		w.add(tag, []byte{'1'})
	} else {
		w.add(tag, []byte{'0'})
	}
}

func (p *Properties) saveUint32(w *rawBlockWriter, offset uintptr, value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	w.add(propOffsetTagMap[offset], buf[:])
}

func (p *Properties) saveUvarint(w *rawBlockWriter, offset uintptr, value uint64) {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], value)
	w.add(propOffsetTagMap[offset], buf[:n])
}

func (p *Properties) saveString(w *rawBlockWriter, offset uintptr, value string) {
	w.add(propOffsetTagMap[offset], []byte(value))
}

func (p *Properties) save(w *rawBlockWriter) {
	p.saveUvarint(w, unsafe.Offsetof(p.RawKeySize), p.RawKeySize)
	p.saveUvarint(w, unsafe.Offsetof(p.RawValueSize), p.RawValueSize)
	p.saveUvarint(w, unsafe.Offsetof(p.DataSize), p.DataSize)
	p.saveUvarint(w, unsafe.Offsetof(p.IndexSize), p.IndexSize)
	if p.IndexPartitions != 0 {
		p.saveUvarint(w, unsafe.Offsetof(p.IndexPartitions), p.IndexPartitions)
		p.saveUvarint(w, unsafe.Offsetof(p.TopLevelIndexSize), p.TopLevelIndexSize)
	}
	p.saveUvarint(w, unsafe.Offsetof(p.IndexKeyIsUserKey), p.IndexKeyIsUserKey)
	p.saveUvarint(w, unsafe.Offsetof(p.NumEntries), p.NumEntries)
	p.saveUvarint(w, unsafe.Offsetof(p.NumRangeDeletions), p.NumRangeDeletions)
	p.saveUvarint(w, unsafe.Offsetof(p.NumDataBlocks), p.NumDataBlocks)
	p.saveUvarint(w, unsafe.Offsetof(p.FilterSize), p.FilterSize)
	p.saveUvarint(w, unsafe.Offsetof(p.FormatVersion), p.FormatVersion)
	p.saveUvarint(w, unsafe.Offsetof(p.FixedKeyLen), p.FixedKeyLen)
	p.saveUvarint(w, unsafe.Offsetof(p.ColumnFamilyID), p.ColumnFamilyID)
	p.saveUvarint(w, unsafe.Offsetof(p.CreationTime), p.CreationTime)
	p.saveUvarint(w, unsafe.Offsetof(p.OldestKeyTime), p.OldestKeyTime)
	if p.FilterPolicyName != "" {
		p.saveString(w, unsafe.Offsetof(p.FilterPolicyName), p.FilterPolicyName)
	}
	if p.ComparatorName != "" {
		p.saveString(w, unsafe.Offsetof(p.ComparatorName), p.ComparatorName)
	}
	if p.MergeOperatorName != "" {
		p.saveString(w, unsafe.Offsetof(p.MergeOperatorName), p.MergeOperatorName)
	}
	if p.PrefixExtractorName != "" {
		p.saveString(w, unsafe.Offsetof(p.PrefixExtractorName), p.PrefixExtractorName)
	}
	if p.PropertyCollectorNames != "" {
		p.saveString(w, unsafe.Offsetof(p.PropertyCollectorNames), p.PropertyCollectorNames)
	}
	if p.ColumnFamilyName != "" {
		p.saveString(w, unsafe.Offsetof(p.ColumnFamilyName), p.ColumnFamilyName)
	}
	if p.CompressionName != "" {
		p.saveString(w, unsafe.Offsetof(p.CompressionName), p.CompressionName)
	}
	p.saveUint32(w, unsafe.Offsetof(p.IndexType), p.IndexType)
	p.saveBool(w, unsafe.Offsetof(p.WholeKeyFiltering), p.WholeKeyFiltering)
	p.saveBool(w, unsafe.Offsetof(p.PrefixFiltering), p.PrefixFiltering)
	p.saveUvarint(w, unsafe.Offsetof(p.GlobalSeqNum), p.GlobalSeqNum)
	p.saveUvarint(w, unsafe.Offsetof(p.Version), p.Version)

	keys := make([]string, 0, len(p.UserProperties))
	for key := range p.UserProperties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		w.add(db.InternalKey{UserKey: []byte(key)}, []byte(p.UserProperties[key]))
	}
}
