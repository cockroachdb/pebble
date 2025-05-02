Here’s a walkthrough of how this “atomic marker” system works, what files it
creates, and what to watch out for on Linux vs. Windows.

---

## 1. What is a “marker”?

A *marker* is simply a file whose name encodes a string value and a
monotonically increasing counter.  By always creating a *new* file for the next
value (and then deleting the old one), the code ensures that at any point
on-disk there is exactly one visible marker file whose name tells you “the
current value is X, at iteration N.”

Filenames have the pattern:

```
marker.<markerName>.<iteration>.<value>
```

- `<markerName>` is an arbitrary identifier you chose.
- `<iteration>` is a zero-padded, 6-digit decimal.
- `<value>` is the new state you’re storing.

For example, if your marker is named `foo`, you might see files like:

```text
marker.foo.000001.alpha
marker.foo.000002.beta
```

---

## 2. High-level API

### ReadMarker(fs, dir, markerName)
- Lists all files in `dir`.
- Picks out those beginning with `marker.` and matching your `markerName`.
- Returns the highest-iteration file’s `value` (e.g. `"beta"`).

### LocateMarker(fs, dir, markerName)
- Same as `ReadMarker`, but also opens `dir` for later syncing.
- Returns a `*Marker` object plus the current value.

---

## 3. Core logic: parsing and scanning

```go
func scanForMarker(fs vfs.FS, ls []string, markerName string) (scannedState, error)
```
- Iterates over filenames in `ls`.
- Calls `parseMarkerFilename`, which:
  1. Strips the `marker.` prefix.
  2. Splits off the name, the iteration (parsed via `strconv.ParseUint`), and the value.
- Keeps track of:
  - **state.filename**: the newest marker file seen so far.
  - **state.iter**: that file’s iteration number.
  - **state.value**: that file’s embedded value.
  - **state.obsolete**: a list of older marker files.

---

## 4. The `Marker` object

Once you have a `*Marker`, you can do two things:

### Move(newValue string)
1. Increment the internal `iter` counter.
2. Build a filename `marker.<name>.<iter>.<newValue>`.
3. Create that file via `fs.Create(...)`.
4. Close it, then delete the old `marker.*` file.
5. Call `dirFD.Sync()` to flush the directory metadata.

If deletion of the old file fails, it’s remembered in an `obsoleteFiles` slice
for later cleanup.

### RemoveObsolete()
Loops over any filenames in `obsoleteFiles` and tries to remove them. Stops at
first error (leaving the remaining entries in `obsoleteFiles`).

---

## 5. Examples of files created

Imagine we start with an empty directory and markerName = `"task"`:

```shell
$ ls
# (empty)

// First LocateMarker → no existing files, iter=0, value=""
marker, _, err := LocateMarker(fs, "/path", "task")

// Move to "started":
marker.Move("started")
$ ls /path
marker.task.000001.started

// Move to "halfway":
marker.Move("halfway")
$ ls /path
marker.task.000002.halfway

// If RemoveObsolete hasn’t run yet, you might still see the old:
marker.RemoveObsolete()
$ ls /path
marker.task.000002.halfway
```

And if your value strings contain dots or special chars, they simply go into the
filename (so avoid `/`).

---

## 6. Linux vs. Windows quirks

1. **Path separators & case-sensitivity**
  - On Linux, files named `marker.Task.000001.foo` and `marker.task.000001.foo` are
    distinct.
  - On Windows, filenames are case-insensitive, so your marker names should avoid
    differing only by letter case.

2. **Directory sync (`dirFD.Sync()`)**
  - Linux: opening a directory and `fsync`-ing its file descriptor reliably flushes the new/removed filenames to disk.
  - Windows: the equivalent “flush directory metadata” is more limited.  In some cases you may see the new file appear immediately but metadata not fully durable until later; and deleting a file that’s still in use can fail.

3. **Deleting open files**
  - Linux lets you `unlink` (remove) a file even while a process still has it open; the file truly goes away only after all handles close.
  - Windows generally refuses to delete an open file handle, so if another process still has the old marker open, the `Remove` call will error.  That error is caught and the old filename pushed into `obsoleteFiles`.

4. **Maximum filename length**
  - Windows MAX_PATH (260 chars) may be hit if your `<value>` is long; Linux allows up to around 255 bytes per component.  Keep your values short.

---

### Summary

- **Naming scheme**: `marker.<name>.<6-digit-iter>.<value>`
- **Read/Locate**: scan the directory, pick the highest iter.
- **Move**: create new marker file, delete old, sync.
- **Cleanup**: `RemoveObsolete()` for any leftovers.
- **Linux vs. Windows**: watch case sensitivity, directory‐sync semantics, and
  open‐file deletion behavior.

This pattern guarantees that at most one marker file is “current,” and that any
failure mid‐move leaves you either at the old value or the new one—never a
corrupted state.
