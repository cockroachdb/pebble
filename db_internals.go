// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// JobID identifies a job (like a compaction). Job IDs are passed to event
// listener notifications and act as a mechanism for tying together the events
// and log messages for a single job such as a flush, compaction, or file
// ingestion. Job IDs are not serialized to disk or used for correctness.
type JobID int

// newJobIDLocked returns a new JobID; DB.mu must be held.
func (d *DB) newJobIDLocked() JobID {
	res := d.mu.nextJobID
	d.mu.nextJobID++
	return res
}

func (d *DB) newJobID() JobID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.newJobIDLocked()
}
