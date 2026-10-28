# How the cache stores objects

What `internal/filestore` does underneath the object cache, for anybody
changing it or wondering why it behaves the way it does. What the cache caches
and when an entry goes stale is in the [README](../README.md#caching); how to
size and run it is in [Running on Kubernetes](kubernetes.md#the-object-cache).

`internal/filestore` holds the bytes: one file per cached object, an index of
them in memory, and nothing else. It is built for a local NVMe volume that
belongs to the process for as long as the process runs, where creating a file
costs microseconds and nothing about the disk has to be worked around. It
knows only opaque keys and opaque payloads: not what S3 is, which tenant
asked, or whether anyone was allowed to.

## Layout

  * **Every object is a file of its own**, named by a sequence number and
    spread over 256 subdirectories. The payload starts at offset 0; the
    stored headers and the key follow it, because an upload's ETag is only
    known once the upload has landed.
  * **A write is published by the index, not by the filesystem.** Nothing
    points at a file until its commit, and a file name is never reused, so no
    reader sees a half-written object and a new version of a key is a new
    file. A reader that already opened the old one reads it to the end.
  * **Nothing is fsynced.** The files only have to be readable by the
    process that wrote them, and the page cache already guarantees that.
  * **An invalidation cannot be lost.** Deleting a key takes it out of the
    index under a lock, which cannot fail; the file is unlinked right after,
    or by the last reader still streaming it. There is no queue that could be
    full and no disk state that could refuse it, so after a write through
    this proxy, no read through it finds the version the write replaced.
  * **Eviction is least recently used, per object, and happens before a
    write rather than after it.**

## The ceiling

`--cache-max-bytes` is the most the cache's files ever occupy on disk. Every
byte is counted from before it is written until after it is gone:

  * A write claims its length before its first byte, evicting the least
    recently used entries first if that is what it takes. A known length is
    claimed in full when the write starts, with room for the stored headers;
    an unknown one is claimed as it arrives.
  * A file that has left the index stays counted until it is unlinked. While
    a reader still has it open, that is when the reader closes it, so a slow
    download of an evicted object keeps its file, and its share of the
    ceiling, until it ends.
  * A write that counts on a removal somebody else is still doing waits for
    it before it writes anything. That takes no longer than an unlink.
  * What a previous process left in the directory is removed before the store
    opens, so it never sits on disk uncounted.

A write that cannot be fitted is refused, and the object is simply not
cached. Writes in flight hold at most half the ceiling between them, or one
`--cache-max-object-size` if that is more, so a burst of misses cannot evict
the whole cache to make room for itself; and nothing is evicted for a write
that would be refused anyway.

Sizes are counted in whole 4 KiB blocks, which is what ext4 and XFS allocate
on a volume formatted with their defaults: a cache of 200-byte objects
counted at 200 bytes each would overrun its ceiling many times over. Not
counted are the 256 shard directories, about 1 MiB, and on XFS the
speculative preallocation it can briefly give a file that is still growing.

`TestTheCeilingHoldsUnderLoad` holds the store to this: it measures the
directory throughout concurrent writers of known and unknown length, readers
that keep files open across evictions, and deletes.

## Every start is a cold start

Opening the cache empties its directory. Nothing a previous process wrote is
read back: there is **no startup scan**, no on-disk index and no recovery, and
that is deliberate. A new pod gets an empty `emptyDir` anyway. The one case in
which old files are still there is a container restarted inside the same pod,
after a crash or an OOM kill, and those files are discarded rather than
trusted. They are removed before the proxy starts serving, which is instant
with a few thousand files and takes a few seconds with hundreds of thousands.

A restart therefore costs a cold cache, and every object is fetched from the
object store once more. Keeping the cache across restarts is not needed today.
If it ever is, it takes two things this store does not have: a marker written
after a `syncfs` at a clean shutdown, so that a start can tell complete files
from ones the page cache never wrote out, and a scan of the directory at
startup to rebuild the index.

## Keys

The index maps the **whole key** to its file: bucket, injected tenant prefix
and object key, exactly as `internal/proxy` built it. Nothing is hashed: two
tenants' `data/a.parquet` are two entries because their keys differ, and no
collision can ever merge them. Each file also records the key it was written
for, and every read compares it: an index entry that pointed at another key's
file would be a miss, never that key's bytes.
