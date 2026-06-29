# lsmstore.py — Lesson 2: memtable + SSTable + WAL (the LSM core)
#
# (The vault lesson calls this module toydb_l2.py; we name it lsmstore.py to
# match logstore.py from L1. Same idea.)
#
# We reuse L0's harness — the canonical SimDisk carries the fault hooks the
# determinism spine is built on. We depend only on the interfaces.
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterator, Optional

from harness import Clock, Disk, KVStore, SimDisk


# ---------------------------------------------------------------------------
# Record encoding. Shared by WAL and SSTable data region (same as L1's format).
#   [flags u8][klen u32][vlen u32][key][value]
# flags bit 0 = tombstone. A tombstone has vlen == 0 and value b"".
# ---------------------------------------------------------------------------

_HDR = struct.Struct("<BII")          # little-endian: flags, klen, vlen
TOMBSTONE = 0x01


@dataclass(frozen=True, slots=True)
class Record:
    key: bytes
    value: bytes          # b"" when tombstone
    tombstone: bool = False


def encode_record(rec: Record) -> bytes:
    flags = TOMBSTONE if rec.tombstone else 0
    return _HDR.pack(flags, len(rec.key), len(rec.value)) + rec.key + rec.value


def decode_record_at(buf: bytes, off: int) -> tuple[Record, int]:
    """Decode the record starting at `off`; return (record, next_offset)."""
    flags, klen, vlen = _HDR.unpack_from(buf, off)
    off += _HDR.size
    key = buf[off:off + klen]; off += klen
    val = buf[off:off + vlen]; off += vlen
    return Record(key, val, bool(flags & TOMBSTONE)), off


# ---------------------------------------------------------------------------
# SSTable footer.   [index_offset u64][index_count u64][MAGIC u32]
# ---------------------------------------------------------------------------

_FOOTER = struct.Struct("<QQI")
FOOTER_SIZE = _FOOTER.size
SST_MAGIC = 0x10A1B1E2                 # "toy table", arbitrary sentinel
SPARSE_EVERY = 4                       # one index entry per 4 data records


# ---------------------------------------------------------------------------
# SSTable WRITER  (Exercise 2.1)
# ---------------------------------------------------------------------------

class SSTableWriter:
    """Writes a sorted iterable of Records to `disk` as data | index | footer."""
    def __init__(self, disk: Disk) -> None:
        self.disk = disk

    def write_all(self, records: Iterator[Record]) -> None:
        # === YOUR CODE (Exercise 2.1) ===
        # 1. Walk `records` (already sorted by key). For each, encode it and
        #    write it at the current append offset. Track the byte offset of
        #    every record.
        # 2. Build a SPARSE index: capture (key, offset) for record #0, #SPARSE_EVERY,
        #    #2*SPARSE_EVERY, ... Store the rest as you go.
        # 3. After all data, write the index region: for each captured pair emit
        #    [klen u32][offset u64][key]. Remember where the index region started
        #    (index_offset) and how many entries it has (index_count).
        # 4. Write the footer: _FOOTER.pack(index_offset, index_count, SST_MAGIC).
        # 5. self.disk.fsync().
        raise NotImplementedError
        # === END ===


# ---------------------------------------------------------------------------
# SSTable READER  (Exercise 2.2 is the point read inside get())
# ---------------------------------------------------------------------------

_IDX = struct.Struct("<IQ")            # klen, offset  (key bytes follow)


class SSTableReader:
    """Reads an SSTable. Loads the sparse index on open; data stays on disk."""
    def __init__(self, disk: Disk) -> None:
        self.disk = disk
        self._index: list[tuple[bytes, int]] = []   # (key, data_offset), sorted
        self._data_end = 0                           # first byte of index region
        self._load_index()

    def _load_index(self) -> None:
        total = self.disk.size()
        foot = self.disk.read(total - FOOTER_SIZE, FOOTER_SIZE)
        index_offset, index_count, magic = _FOOTER.unpack(foot)
        if magic != SST_MAGIC:
            raise ValueError("bad SSTable magic; corrupt or wrong file")
        self._data_end = index_offset
        raw = self.disk.read(index_offset, total - FOOTER_SIZE - index_offset)
        off = 0
        for _ in range(index_count):
            klen, data_off = _IDX.unpack_from(raw, off); off += _IDX.size
            key = raw[off:off + klen]; off += klen
            self._index.append((key, data_off))

    def get(self, key: bytes) -> Optional[Record]:
        """Return the Record for `key` (possibly a tombstone), or None if this
        SSTable does not contain the key at all."""
        # === YOUR CODE (Exercise 2.2) ===
        # 1. Binary-search self._index for the largest indexed key <= `key`.
        #    That gives the data offset to start scanning from. If `key` is
        #    smaller than every indexed key, it can't be here -> return None.
        # 2. Scan forward record-by-record from that offset (use the cached
        #    block self.disk.read(start, self._data_end - start)) until:
        #       - rec.key == key  -> return rec  (tombstone or not)
        #       - rec.key  > key   -> not present -> return None
        #       - you reach self._data_end -> return None
        raise NotImplementedError
        # === END ===

    def __iter__(self) -> Iterator[Record]:
        """Yield all records in sorted order (used by scan and, later, compaction)."""
        block = self.disk.read(0, self._data_end)
        off = 0
        while off < len(block):
            rec, off = decode_record_at(block, off)
            yield rec


# ---------------------------------------------------------------------------
# WAL  (Exercise 2.4)
# ---------------------------------------------------------------------------

class WAL:
    """Append-only durability log in front of the memtable. This is L1's log."""
    def __init__(self, disk: Disk) -> None:
        self.disk = disk

    def append(self, rec: Record) -> None:
        # === YOUR CODE (Exercise 2.4a) ===
        # Encode `rec` and append it at the end of the WAL disk, then fsync so
        # the write is durable BEFORE we return (the caller inserts into the
        # memtable only after this returns).
        raise NotImplementedError
        # === END ===

    def replay(self) -> Iterator[Record]:
        # === YOUR CODE (Exercise 2.4b) ===
        # Read the whole WAL and yield every Record in append order. (On open,
        # the engine feeds these back into a fresh memtable.)
        raise NotImplementedError
        # === END ===

    def truncate(self) -> None:
        self.disk.truncate(0)
        self.disk.fsync()


# ---------------------------------------------------------------------------
# The LSM engine
# ---------------------------------------------------------------------------

class LSMStore(KVStore):
    """Memtable (dict, sorted on flush) + WAL + a stack of SSTables.

    `disk_factory(name)` returns a fresh Disk for a given logical file name, so
    the engine can create WALs ('wal') and SSTables ('sst-0', 'sst-1', ...).
    Pass the SAME factory across a crash to get durability replay for free.
    """
    def __init__(self, disk_factory, clock: Clock, max_bytes: int = 4096) -> None:
        self._disk_factory = disk_factory
        self._clock = clock
        self._max_bytes = max_bytes

        self._mem: dict[bytes, Record] = {}      # newest writes
        self._mem_bytes = 0
        self._ssts: list[SSTableReader] = []      # index 0 = oldest, last = newest
        self._next_sst = 0

        # Recover any SSTables already on disk, then replay the WAL.
        self._recover_ssts()
        self._wal_disk = disk_factory("wal")
        self._wal = WAL(self._wal_disk)
        for rec in self._wal.replay():
            self._mem[rec.key] = rec              # replay reconstructs the memtable
            self._mem_bytes += len(rec.key) + len(rec.value)  # keep auto-flush accurate

    def _recover_ssts(self) -> None:
        # Naive contiguous scan (sst-0, sst-1, ... until a gap); replaced by the
        # manifest in L5, which tolerates gaps from compaction.
        i = 0
        while True:
            disk = self._disk_factory(f"sst-{i}")
            if disk.size() == 0:
                disk.close()
                break
            self._ssts.append(SSTableReader(disk))
            i += 1
        self._next_sst = i

    # ----- write path -----
    def put(self, key: bytes, value: bytes) -> None:
        self._apply(Record(key, value, tombstone=False))

    def delete(self, key: bytes) -> None:
        self._apply(Record(key, b"", tombstone=True))

    def _apply(self, rec: Record) -> None:
        self._wal.append(rec)                      # 1. durable first
        self._mem[rec.key] = rec                   # 2. then queryable
        self._mem_bytes += len(rec.key) + len(rec.value)
        if self._mem_bytes >= self._max_bytes:
            self.flush()

    # ----- read path  (Exercise 2.3) -----
    def get(self, key: bytes) -> Optional[bytes]:
        # === YOUR CODE (Exercise 2.3) ===
        # Newest -> oldest, first hit wins:
        # 1. Memtable: if key in self._mem -> it's a hit. Tombstone -> None,
        #    else return its value. STOP either way.
        # 2. SSTables newest -> oldest (iterate self._ssts in REVERSE). For each,
        #    rec = sst.get(key); if rec is not None it's a hit: tombstone -> None,
        #    else return rec.value. STOP at the first hit.
        # 3. No hit anywhere -> return None.
        raise NotImplementedError
        # === END ===

    # ----- flush  (Exercise 2.5) -----
    def flush(self) -> None:
        if not self._mem:
            return
        # === YOUR CODE (Exercise 2.5) ===
        # 1. Freeze: take self._mem's records sorted by key.
        # 2. Open a fresh disk for f"sst-{self._next_sst}" and write the sorted
        #    records with SSTableWriter.
        # 3. Open an SSTableReader over that disk and append it to self._ssts
        #    (it becomes the NEWEST on-disk run).
        # 4. Bump self._next_sst. Truncate the WAL (its contents are now durable
        #    in the SSTable). Reset self._mem and self._mem_bytes.
        raise NotImplementedError
        # === END ===

    # ----- range scan: merge memtable + all SSTables, newest wins -----
    def scan(self, start: bytes, end: bytes) -> Iterator[tuple[bytes, bytes]]:
        # Materializes everything for simplicity; the streaming k-way merge that
        # exploits the sorted runs is L3.
        merged: dict[bytes, Record] = {}
        for sst in self._ssts:                     # oldest first ...
            for rec in sst:
                merged[rec.key] = rec
        merged.update(self._mem)                    # ... memtable overwrites (newest)
        for key in sorted(merged):
            if start <= key < end:
                rec = merged[key]
                if not rec.tombstone:
                    yield key, rec.value

    def close(self) -> None:
        self._wal_disk.close()
        for sst in self._ssts:
            sst.disk.close()


# ---------------------------------------------------------------------------
# Disk wiring: a dict of SimDisks keyed by logical name. The dict SURVIVES a
# crash (the bytes persist), so reopening over the same dict gives durability
# replay. To simulate a crash: drop the LSMStore, keep the dict, reopen.
# ---------------------------------------------------------------------------

def make_factory(store: dict[str, SimDisk]):
    def factory(name: str) -> SimDisk:
        if name not in store:
            store[name] = SimDisk()
        return store[name]
    return factory
