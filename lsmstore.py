# lsmstore.py — Lesson 2.2: the LSM engine over sstable.py (NO durability yet)
#
# Builds directly on L2.1's SSTable (imported, never rewritten). This is the
# Store: it composes a RAM memtable + a stack of immutable SSTables and serves the
# KVStore interface over all of them.
#
#   write:  insert into the memtable (RAM only -- see the durability gap below)
#   flush:  freeze the sorted memtable into a new SSTable, reset the memtable
#   read:   memtable, then SSTables newest->oldest; first hit wins; tombstone -> None
#
# DURABILITY GAP (deliberate, the L2.2 cliffhanger): writes live only in the
# memtable until a flush freezes them into an SSTable. A crash before flush loses
# them -- and a delete-after-flush-then-crash RESURRECTS the key. L2.3 closes this
# by re-introducing L1's append-only log here, demoted to a write-ahead log.
from __future__ import annotations

from typing import Iterator, Optional

from harness import Clock, KVStore, SimDisk
from sstable import Record, SSTableReader, SSTableWriter


class LSMStore(KVStore):
    """Memtable (dict, sorted on flush) + a stack of SSTables.

    `disk_factory(name)` returns a fresh Disk for a logical file name, so the
    engine can create SSTables ('sst-0', 'sst-1', ...). Pass the SAME factory
    across a reopen to recover the SSTables already on disk.
    """
    def __init__(self, disk_factory, clock: Clock, max_bytes: int = 4096) -> None:
        self._disk_factory = disk_factory
        self._clock = clock
        self._max_bytes = max_bytes

        self._mem: dict[bytes, Record] = {}        # newest writes (RAM only)
        self._mem_bytes = 0
        self._ssts: list[SSTableReader] = []        # index 0 = oldest, last = newest
        self._next_sst = 0

        # Recover SSTables already on disk. (No WAL replay yet -- that's L2.3, and
        # its absence is exactly why a crash before flush loses the memtable.)
        self._recover_ssts()

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

    # ----- write path (provided: memtable-only, no durability) -----
    def put(self, key: bytes, value: bytes) -> None:
        self._apply(Record(key, value, tombstone=False))

    def delete(self, key: bytes) -> None:
        self._apply(Record(key, b"", tombstone=True))

    def _apply(self, rec: Record) -> None:
        # L2.2: straight into the memtable, nothing durable. L2.3 prepends a
        # WAL-first append here (durable before queryable).
        self._mem[rec.key] = rec
        self._mem_bytes += len(rec.key) + len(rec.value)
        if self._mem_bytes >= self._max_bytes:
            self.flush()

    # ----- read path  (Exercise 2.2a) -----
    def get(self, key: bytes) -> Optional[bytes]:
        # === YOUR CODE (Exercise 2.2a) ===
        # Newest -> oldest, first hit wins:
        # 1. Memtable: if key in self._mem -> hit. tombstone -> None, else its
        #    value (rec.value). STOP either way.
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
        # 4. Reset self._mem and self._mem_bytes.
        raise NotImplementedError
        # === END ===

    # ----- range scan: merge memtable + all SSTables, newest wins -----
    def scan(self, start: bytes, end: bytes) -> Iterator[tuple[bytes, bytes]]:
        # Materializes everything for simplicity; the streaming k-way merge that
        # exploits the sorted runs is L3.
        merged: dict[bytes, Record] = {}
        for sst in self._ssts:                      # oldest first ...
            for rec in sst:
                merged[rec.key] = rec
        merged.update(self._mem)                     # ... memtable overwrites (newest)
        for key in sorted(merged):
            if start <= key < end:
                rec = merged[key]
                if not rec.tombstone:
                    yield key, rec.value

    def close(self) -> None:
        for sst in self._ssts:
            sst.disk.close()


# ---------------------------------------------------------------------------
# Disk wiring: a dict of SimDisks keyed by logical name. The dict SURVIVES a
# crash (the bytes persist), so reopening over the same dict recovers the
# SSTables. To simulate a crash: drop the LSMStore, keep the dict, reopen.
# ---------------------------------------------------------------------------

def make_factory(store: dict[str, SimDisk]):
    def factory(name: str) -> SimDisk:
        if name not in store:
            store[name] = SimDisk()
        return store[name]
    return factory
