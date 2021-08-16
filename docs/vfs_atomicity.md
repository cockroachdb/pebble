# VFS Atomicity

## Background

Pebble's vfs package maintains a virtual filesystem interface through
which both Pebble and CockroachDB code interacts with the filesystem.
The most common implementations are `vfs.Default` and `*vfs.MemFS`. They
both implement atomicity semantics mirroring typical operating system
guarantees. Most notably, `Rename` provides atomicity even if the
destination path already exists. This guarantees reads of the
destination path will read the old file or the new file, and nothing
else, even if the machine crashes in the middle of a rename.

There are several additional implementations of the VFS interface, many
of which wrap other virtual filesystems with additional functionality.
CockroachDB's enterprise encryption provides another VFS implementation
`*engineccl.encryptedFS` that encrypts and decrypts files on the fly.
This `encryptedFS` implementation does not provide the same atomicity
guarantees, because renames involve updating both internal metadata
around encryption keys _and_ the filesystem. Rather than develop a
subtle scheme to ensure encryption-at-rest and all other virtual
filesystem implementations implement the same atomicity semantics, it
was decided to relax the VFS semantic guarantees.

The vast majority of filesystem operations in both Pebble and
CockroachDB do not require stringent atomicity guarantees for link and
rename operations, because they rely on other atomically-updated state
to record updates (eg, manifest writes).

There are a handful of remaining instances that require an atomic link
or rename. This document records all known instances and the mechanism
through which we ensure atomicity.

## Manifest rotations

Pebble records most high-level state (sstable additions, removals,
unflushed log numbers, etc) in a MANIFEST-XXXXXX file. Writes to the
manifest are atomic. When a manifest grows too large, Pebble must rotate
to a new manifest file. This rotation must be atomic.

In RocksDB, and previously in Pebble, this happened through the atomic
update of a file with the fixed filename `CURRENT`. When a new manifest
had been written and Pebble was ready to make the atomic switch to the
new manifest, Pebble would write the new manifest's filename to a
temporary file. It then performed an atomic rename, replacing the
existing `CURRENT` file with the new one. Pebble relied on the atomicity
of the rename operation.

When CockroachDB runs with encryption-at-rest, as described above, the
VFS's rename operation was not atomic. A crash mid-rename might leave
the `CURRENT` file containing undecipherable garbage, preventing the
database from being opened again.

**Fix**: The `CURRENT` file does not contain user data, and there is no
need for the file to be encrypted. Pull request pebble#1227 introduced a
mechanism for Pebble to ask its configured `vfs.FS` if it supports
atomic renames and `Unwrap()` the `vfs.FS` if it does not. Through this
`Unwrap()`-ing, Pebble is able to retrieve an underlying `vfs.FS` that
does perform atomic renames.

Pebble uses this VFS that supports atomic renames for the purpose of
storing exclusively the `CURRENT` file. In the context of CockroachDB
encryption-at-rest, this means that Pebble uses the unencrypted base
filesystem for the `CURRENT` file and the encrypted filesystem for
everything else.

This change is backwards incompatible, because previous databases
running with encryption-at-rest have encrypted `CURRENT` files. To
accommodate this, Pebble uses a new filename `CURRENT-000002` when it
writes to the unencrypted, atomic filesystem. CockroachDB and other
Pebble users must explicitly migrate to the new method of rotating
manifests by ratcheting up a Pebble 'format major version'. See the
comments on `RatchetFormatMajorVersion` and
`Options.FormatMajorVersion`. Once a database has been migrated to the
new format major version that uses the new manifest rotation scheme, it
cannot be opened by earlier Pebble versions.

## Encryption-at-rest `COCKROACH_DATA_KEYS`

CockroachDB's encryption-at-rest implementation has a concept of 'data
keys'. Data keys are cryptographic keys that are used to encrypt files
containing user data. The active data key may be rotated regularly.
CockroachDB needs to maintain a record of all the data keys that are
still required to decrypt files. CockroachDB maintains this record in a
file previously called `COCKROACH_DATA_KEYS`.

The `COCKROACH_DATA_KEYS` file is encrypted using the cryptographic keys
called 'store keys' directly supplied by the operator. To accomplish
this encryption, the `engineccl.DataKeyManager` uses an `*encryptedFS`
to read and write the `COCKROACH_DATA_KEYS` file. Whenever the data key
manager needs to update the file to add or remove a data key, it writes
the new file state to a temporary file and performs a rename to the
`COCKROACH_DATA_KEYS` path. These filesystem operations are performed
using an `*encryptedFS`, meaning the rename is not atomic. A crash
mid-rename may leave the `COCKROACH_DATA_KEYS` file containing
undecipherable garbage.

**Fix**: Pull request cockroachdb/cockroach#69390 modifies the
`engineccl.DataKeyManager` to write to a new file every time, rather
than continually replacing the `COCKROACH_DATA_KEYS` file. A numeric
portion is added to the filename (`COCKROACHDB_DATA_KEYS-0000000001`)
containing a monotonically increasing counter.

After the new data keys registry is written to the `*encryptedFS`, the
`DataKeyManager` calls into the `*storage.PebbleFileRegistry` to
atomically write a record of the new data keys registry filename to the
file registry.

On load, the data key managers retrieves the most recent record that
contains the data keys registry filename, and initializes its state from
that filename.

## Encryption-at-rest file registry

CockroachDB's encryption-at-rest implementation maintains a file called
the file registry. This file maintains a record of all the files under
the purview of encryption-at-rest, alongside non-sensitive metadata
necessary for the decryption of the files. When this file grows too
large, it must be rotated to a new file. This is performed with a rename
operation.

**No fix necessary**: The rename of the file registry during rotations
is atomic. The file registry is stored unencrypted on the base
filesystem. The base filesystem provides rename atomicity guarantees,
so using the rename in this fashion is ok.

TODO(jackson): We should still assert that the filesystem provides
atomic renames once the vfs.Attributes change is available.

## Storage `STORAGE_MIN_VERSION`

The CockroachDB storage package maintains a file called
`STORAGE_MIN_VERSION` containing the minimum version that the storage
engine must maintain compatibility with. This is used to avoid
unnecessarily supporting the binary's minimum version if the cluster has
already finalized the new version.

**Fix**: During version finalization, this file must be atomically
replaced. This happens through a `Rename` operation. However, the
`vfs.FS` used is always the unencrypted base FS as of
cockroachdb/cockroach#69432.
