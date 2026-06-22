# logstore.py — Lesson 1: append-only log + in-memory hash index
from __future__ import annotations

import struct
from typing import Iterator, Optional

# From L0. We depend only on the Disk and KVStore interfaces, never on a
# concrete RealDisk/SimDisk — that is what makes the torn-write test possible.
from harness import Disk, KVStore

# On-disk record header: <  little-endian
#                        B  flags (bit0 = tombstone)
#                        I  key length  (u32)
#                        I  value length (u32)
HEADER = struct.Struct("<BII")
assert HEADER.size == 9

FLAG_TOMBSTONE = 0x01


class LogStore(KVStore):
    """A durable KVStore backed by an append-only log on an injected Disk.

    The on-disk file is the source of truth. `self.index` maps key -> byte
    offset of the key's most recent record, and is rebuilt from the file on
    open. It is never persisted.
    """

    def __init__(self, disk: Disk, *, fsync_on_write: bool = True) -> None:
        self.disk = disk
        self.fsync_on_write = fsync_on_write
        self.index: dict[bytes, int] = {}

    @classmethod
    def open(cls, disk: Disk, *, fsync_on_write: bool = True) -> "LogStore":
        """Open a store over `disk`, recovering the index from existing bytes."""
        store = cls(disk, fsync_on_write=fsync_on_write)
        store.rebuild_index()
        return store

    # ---- record encoding helpers -------------------------------------------
    # Provided, so the framing is explicit and unambiguous. You will *use*
    # these in append() and rebuild_index(); you do not need to change them.

    @staticmethod
    def encode(key: bytes, value: bytes, *, tombstone: bool) -> bytes:
        """Serialize one record to bytes."""
        flags = FLAG_TOMBSTONE if tombstone else 0
        val = b"" if tombstone else value
        return HEADER.pack(flags, len(key), len(val)) + key + val

    @staticmethod
    def record_len(klen: int, vlen: int) -> int:
        """Total on-disk size of a record with the given key/value lengths."""
        return HEADER.size + klen + vlen

    # ---- write path --------------------------------------------------------

    def append(self, key: bytes, value: bytes, *, tombstone: bool = False) -> int:
        """Encode one record and append it at the end of the log.

        Returns the byte offset at which the record was written (its index
        entry). Must write through self.disk and respect self.fsync_on_write.
        Do NOT update self.index here — put()/delete() own that.
        """
        # === YOUR CODE (Exercise 1.1) ===
        raise NotImplementedError
        # === END ===

    def rebuild_index(self) -> None:
        """Scan the whole file front-to-back and rebuild self.index.

        Last write wins (the file is in chronological order). A tombstone
        removes the key. A torn/truncated trailing record (header or payload
        runs past EOF) must be dropped silently — do NOT raise.
        """
        self.index = {}
        # === YOUR CODE (Exercise 1.2) ===
        raise NotImplementedError
        # === END ===

    # ---- KVStore interface -------------------------------------------------

    def get(self, key: bytes) -> Optional[bytes]:
        """Return the latest value for key, or None if absent/tombstoned."""
        # === YOUR CODE (Exercise 1.3) ===
        raise NotImplementedError
        # === END ===

    def put(self, key: bytes, value: bytes) -> None:
        offset = self.append(key, value, tombstone=False)
        self.index[key] = offset

    def delete(self, key: bytes) -> None:
        self.append(key, b"", tombstone=True)
        self.index.pop(key, None)

    def scan(self, start: bytes, end: bytes) -> Iterator[tuple[bytes, bytes]]:
        # The hash index is unordered, so range scan means sort the live keys.
        # (This is a real weakness of hash-indexed logs: no efficient range
        # queries. Sorted SSTables in L2 fix it. For now we sort in memory.)
        for key in sorted(k for k in self.index if start <= k < end):
            value = self.get(key)
            if value is not None:
                yield key, value

    def close(self) -> None:
        self.disk.fsync()
        self.disk.close()
