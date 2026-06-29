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

from threading import Lock

from collections import namedtuple
Record = namedtuple('Record', ['flags', 'klen', 'vlen', 'keybytes', 'valbytes'])

DEBUG = False
def pml(msg):
    # pml: poor man's log
    if DEBUG:
        print(msg)

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

        self.write_lock = Lock()

    @property
    def _bytesize(self):
        return self.disk.size() # i'm not sure this is correct for a real disk
        # return sum(v for _, v in self.index.items())

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
        # 1. take a write lock on the log
        with self.write_lock:
            # 2. manifest the bytes to be written by calling encode()
            pml(f'writing {key} {value} {tombstone}')
            bytes_to_write = self.encode(key, value, tombstone=tombstone)
            location_to_write = self._bytesize
            # 3. write
            pml(f' . to location {location_to_write} {bytes_to_write}')
            self.disk.write(location_to_write, bytes_to_write)
            # fsync if necessary (TODO: what are the failure modes here? fsync fails but write succeeds?)
            if self.fsync_on_write:
                self.disk.fsync()
        
            # 4. return the offset
            return location_to_write
        # === END ===

    def _record_from_disk_offset(self, offset: int) -> Record:
        pml(f'read record at offset {offset}')
        headerbytes = self.disk.read(offset, HEADER.size)

        pml(f'  got {headerbytes}')
        if not headerbytes or len(headerbytes) != HEADER.size:
            # record header not constructible, or truncation problem, no readable record.
            # TODO truncate the file, return None
            self.disk.truncate(offset)
            return None

        header = HEADER.unpack(headerbytes)
        (flags, klen, vlen) = header
        
        pml(f' read header: {flags} {klen} {vlen}; will read key then value')
        keybytes = self.disk.read(offset + HEADER.size, klen)
        pml(f'  read keybytes: {keybytes}')
        if len(keybytes) != klen:
            # TODO truncate the file, return None
            self.disk.truncate(offset)
            return None
        
        valbytes = self.disk.read(offset + HEADER.size + klen, vlen)
        pml(f'  read valbytes: {valbytes}')
        if len(valbytes) != vlen:
            # TODO truncate the file, return None
            self.disk.truncate(offset)
            return None
        
        return Record(flags, klen, vlen, keybytes, valbytes)
        
    def rebuild_index(self) -> None:
        """Scan the whole file front-to-back and rebuild self.index.

        Last write wins (the file is in chronological order). A tombstone
        removes the key. A torn/truncated trailing record (header or payload
        runs past EOF) must be dropped silently — do NOT raise.
        """
        self.index = {}
        # === YOUR CODE (Exercise 1.2) ===
        curoffset = 0
        pml("rebuilding index")

        record = self._record_from_disk_offset(curoffset)
        while record:
            # index maps key bytes to the offset where we read the HEADER
            pml(f'  update index {record.keybytes}: {curoffset}')
            self.index[record.keybytes] = curoffset
                    
            curoffset += self.record_len(record.klen, record.vlen)
            pml(f'  new offset: {curoffset}')
            record = self._record_from_disk_offset(curoffset)
                
        pml('done building idx')

    # ---- KVStore interface -------------------------------------------------

    def get(self, key: bytes) -> Optional[bytes]:
        """Return the latest value for key, or None if absent/tombstoned."""
        # === YOUR CODE (Exercise 1.3) ===
        pml(f' getting {key}. cur idx: {self.index}')
        if key not in self.index or self.index[key] == b'':
            return None
        record = self._record_from_disk_offset(self.index[key])
        if not record:
            return None

        if record.flags & FLAG_TOMBSTONE != 0:
            return None        

        return record.valbytes

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
