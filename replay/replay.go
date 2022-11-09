// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package replay

import (
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// loadFlushedSSTableKeys copies keys from the sstables specified by `fileNums`
// in the directory specified by `path` into the provided the batch. Keys are
// applied to the batch in the order dictated by their sequence numbers within
// the sstables, ensuring the relative relationship between sequence numbers is
// maintained.
func loadFlushedSSTableKeys(
	b *pebble.Batch, fs vfs.FS, path string, fileNums []base.FileNum, readOpts sstable.ReaderOptions,
) error {
	// Load all the keys across all the sstables.
	var alloc bytealloc.A
	var keys flushedKeys
	for _, fileNum := range fileNums {
		if err := func() error {
			filePath := base.MakeFilepath(fs, path, base.FileTypeTable, fileNum)
			f, err := fs.Open(filePath)
			if err != nil {
				return err
			}
			r, err := sstable.NewReader(f, readOpts)
			if err != nil {
				return err
			}
			defer r.Close()

			// Load all the point keys.
			if iter, err := r.NewIter(nil, nil); err != nil {
				return err
			} else {
				defer iter.Close()
				for k, lv := iter.First(); k != nil; k, lv = iter.Next() {
					var key flushedKey
					key.Trailer = k.Trailer
					alloc, key.UserKey = alloc.Copy(k.UserKey)
					if v, callerOwned, err := lv.Value(nil); err != nil {
						return err
					} else if callerOwned {
						key.value = v
					} else {
						alloc, key.value = alloc.Copy(v)
					}
					keys = append(keys, key)
				}
			}

			// Load all the range tombstones.
			if iter, err := r.NewRawRangeDelIter(); err != nil {
				return err
			} else if iter != nil {
				defer iter.Close()
				for s := iter.First(); s != nil; s = iter.Next() {
					if err := rangedel.Encode(s, func(k base.InternalKey, v []byte) error {
						var key flushedKey
						key.Trailer = k.Trailer
						alloc, key.UserKey = alloc.Copy(k.UserKey)
						alloc, key.value = alloc.Copy(v)
						keys = append(keys, key)
						return nil
					}); err != nil {
						return err
					}
				}
			}

			// Load all the range keys.
			if iter, err := r.NewRawRangeKeyIter(); err != nil {
				return err
			} else if iter != nil {
				defer iter.Close()
				for s := iter.First(); s != nil; s = iter.Next() {
					if err := rangekey.Encode(s, func(k base.InternalKey, v []byte) error {
						var key flushedKey
						key.Trailer = k.Trailer
						alloc, key.UserKey = alloc.Copy(k.UserKey)
						alloc, key.value = alloc.Copy(v)
						keys = append(keys, key)
						return nil
					}); err != nil {
						return err
					}
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	// Sort the flushed keys by their sequence numbers so that we can apply them
	// to the batch in the same order, maintaining the relative relationship
	// between keys.
	sort.Sort(keys)

	// Add the keys to the batch in the order they were committed when the
	// workload was captured.
	for i := 0; i < len(keys); i++ {
		var err error
		switch keys[i].Kind() {
		case base.InternalKeyKindDelete:
			err = b.Delete(keys[i].UserKey, nil)
		case base.InternalKeyKindSet, base.InternalKeyKindSetWithDelete:
			err = b.Set(keys[i].UserKey, keys[i].value, nil)
		case base.InternalKeyKindMerge:
			err = b.Merge(keys[i].UserKey, keys[i].value, nil)
		case base.InternalKeyKindSingleDelete:
			err = b.SingleDelete(keys[i].UserKey, nil)
		case base.InternalKeyKindRangeDelete:
			err = b.DeleteRange(keys[i].UserKey, keys[i].value, nil)
		case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
			s, err := rangekey.Decode(keys[i].InternalKey, keys[i].value, nil)
			if err != nil {
				return err
			}
			if len(s.Keys) != 1 {
				return errors.Newf("range key span unexpectedly contains %d keys", len(s.Keys))
			}
			switch keys[i].Kind() {
			case base.InternalKeyKindRangeKeySet:
				err = b.RangeKeySet(s.Start, s.End, s.Keys[0].Suffix, s.Keys[0].Value, nil)
			case base.InternalKeyKindRangeKeyUnset:
				err = b.RangeKeyUnset(s.Start, s.End, s.Keys[0].Suffix, nil)
			case base.InternalKeyKindRangeKeyDelete:
				err = b.RangeKeyDelete(s.Start, s.End, nil)
			default:
				err = errors.Newf("unexpected key kind %q", keys[i].Kind())
			}
		default:
			err = errors.Newf("unexpected key kind %q", keys[i].Kind())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type flushedKeys []flushedKey

func (s flushedKeys) Len() int           { return len(s) }
func (s flushedKeys) Less(i, j int) bool { return s[i].Trailer < s[j].Trailer }
func (s flushedKeys) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type flushedKey struct {
	base.InternalKey
	value []byte
}
