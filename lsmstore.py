# lsmstore.py — Lessons 2.2 + 2.3: the LSM engine over sstable.py
#
# Builds directly on L2.1's SSTable (imported, never rewritten). This file is the
# Store: it composes a RAM memtable + a WAL (L1's append log, reused) + a stack of
# immutable SSTables, and serves the KVStore interface over all of them.
#
#   write:  append to WAL (durable), then insert into the memtable
#   flush:  freeze the sorted memtable into a new SSTable, truncate the WAL
#   read:   memtable, then SSTables newest->oldest; first hit wins; tombstone -> None
from __future__ import annotations

from typing import Iterator, Optional

from harness import Clock, Disk, KVStore, SimDisk
from sstable import (
    Record,
    SSTableReader,
    SSTableWriter,
    decode_record_at,
    encode_record,
)


# ---------------------------------------------------------------------------
# WAL  (Exercises 2.3) — L1's append-only log, used purely as a durability buffer.
# No index, no point read: the memtable IS the in-RAM view of the WAL's contents.
# ---------------------------------------------------------------------------

class WAL:
    """Append-only durability log in front of the memtable."""
    def __init__(self, disk: Disk) -> None:
        self.disk = disk

    def append(self, rec: Record) -> None:
        # === YOUR CODE (Exercise 2.3a) ===
        # Encode `rec` and append it at the end of the WAL, then fsync so the
        # write is durable BEFORE returning (the caller inserts into the memtable
        # only after this returns). Use encode_record + disk.write at disk.size().
        raise NotImplementedError
        # === END ===

    def replay(self) -> Iterator[Record]:
        # === YOUR CODE (Exercise 2.3b) ===
        # Read the whole WAL and yield every Record in append order (raw stream --
        # tombstones INCLUDED; do not dedupe). On open the engine feeds these back
        # into a fresh memtable. Use decode_record_at to walk the buffer.
        raise NotImplementedError
        # === END ===

    def truncate(self) -> None:
        # Provided. Called after a flush: the WAL's contents are now durable in an
        # SSTable, so the log resets to empty.
        self.disk.truncate(0)
        self.disk.fsync()


# ---------------------------------------------------------------------------
# The LSM engine
# ---------------------------------------------------------------------------

class LSMStore(KVStore):
    """Memtable (dict, sorted on flush) + WAL + a stack of SSTables.

    `disk_factory(name)` returns a fresh Disk for a logical file name, so the
    engine can create the WAL ('wal') and SSTables ('sst-0', 'sst-1', ...). Pass
    the SAME factory across a crash to get durability replay for free.
    """
    def __init__(self, disk_factory, clock: Clock, max_bytes: int = 4096) -> None:
        self._disk_factory = disk_factory
        self._clock = clock
        self._max_bytes = max_bytes

        self._mem: dict[bytes, Record] = {}       # newest writes
        self._mem_bytes = 0
        self._ssts: list[SSTableReader] = []       # index 0 = oldest, last = newest
        self._next_sst = 0

        # Recover SSTables already on disk, then replay the WAL into the memtable.
        self._recover_ssts()
        self._wal_disk = disk_factory("wal")
        self._wal = WAL(self._wal_disk)
        for rec in self._wal.replay():             # <-- needs Exercise 2.3b to construct
            self._mem[rec.key] = rec
            self._mem_bytes += len(rec.key) + len(rec.value)

    def _recover_ssts(self) -> None:
        # Naive contiguous scan (sst-0, sst-1, ... until a gap). Replaced by a
        # manifest in L5, which tolerates the gaps compaction leaves.
        i = 0
        while True:
            disk = self._disk_factory(f"sst-{i}")
            if disk.size() == 0:
                disk.close()
                break
            self._ssts.append(SSTableReader(disk))
            i += 1
        self._next_sst = i

    # ----- write path (provided: WAL-first ordering) -----
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

    # ----- read path  (Exercise 2.2a) -----
    def get(self, key: bytes) -> Optional[bytes]:
        # === YOUR CODE (Exercise 2.2a) ===
        # Newest -> oldest, first hit wins:
        # 1. Memtable: if key in self._mem -> hit. tombstone -> None, else its
        #    value. STOP either way.
        # 2. SSTables newest -> oldest (iterate self._ssts in REVERSE). For each,
        #    rec = sst.get(key); if rec is not None it's a hit: tombstone -> None,
        #    else rec.value. STOP at the first hit.
        # 3. No hit anywhere -> None.
        raise NotImplementedError
        # === END ===

    # ----- flush  (Exercise 2.2b) -----
    def flush(self) -> None:
        if not self._mem:
            return
        # === YOUR CODE (Exercise 2.2b) ===
        # 1. Freeze: take self._mem's records sorted by key.
        # 2. Open a fresh disk for f"sst-{self._next_sst}" and write the sorted
        #    records with SSTableWriter.
        # 3. Open an SSTableReader over that disk; append it to self._ssts (it
        #    becomes the NEWEST run). Bump self._next_sst.
        # 4. Truncate the WAL (contents now durable in the SSTable). Reset
        #    self._mem and self._mem_bytes.
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
