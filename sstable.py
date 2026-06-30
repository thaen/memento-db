# sstable.py — Lesson 2.1: the SSTable, an immutable sorted run
#
# This is a STANDALONE artifact: no memtable, no WAL, no engine yet. An SSTable
# is a frozen, sorted-on-disk file of records, written once and never edited.
#
# Why it exists (the limit it lifts off L1): L1's LogStore keeps ONE hash-index
# entry per live key, resident in RAM and unordered. That caps how many keys fit
# and makes range scans impossible. The SSTable fixes both: data is laid out
# physically sorted, and the in-RAM index is SPARSE -- one entry per SPARSE_EVERY
# records. You binary-search the sparse index to a neighborhood, then scan
# forward a few records. Sub-linear RAM, sorted layout for free.
#
# File layout:   [ data region ] [ sparse index region ] [ footer ]
#   data:   records back-to-back, sorted by key
#   index:  one (key, data_offset) per SPARSE_EVERY records
#   footer: [index_offset u64][index_count u64][MAGIC u32]
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterator, Optional

from harness import Disk

# ---------------------------------------------------------------------------
# Record encoding (same framing as L1's log: length-prefixed, tombstone bit).
#   [flags u8][klen u32][vlen u32][key][value]
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
# Footer + sparse-index framing (provided).
# ---------------------------------------------------------------------------

_FOOTER = struct.Struct("<QQI")
FOOTER_SIZE = _FOOTER.size
SST_MAGIC = 0x10A1B1E2                 # "toy table" sentinel; guards against junk
SPARSE_EVERY = 4                       # one index entry per 4 data records
_IDX = struct.Struct("<IQ")            # klen, offset  (key bytes follow)


# ---------------------------------------------------------------------------
# WRITER  (Exercise 2.1.1)
# ---------------------------------------------------------------------------

class SSTableWriter:
    """Write a sorted iterable of Records to `disk` as data | index | footer.

    Contract: `records` arrive ALREADY sorted by key (the caller -- a flush --
    owns the sort). This class only frames and indexes them.
    """
    def __init__(self, disk: Disk) -> None:
        self.disk = disk

    def write_all(self, records: Iterator[Record]) -> None:
        # === YOUR CODE (Exercise 2.1.1) ===
        # 1. Walk `records` (already sorted). For each, encode it and append it
        #    at the current data offset. Track each record's byte offset.
        # 2. Build a SPARSE index: capture (key, offset) for record #0,
        #    #SPARSE_EVERY, #2*SPARSE_EVERY, ...  (Record #0 must ALWAYS be
        #    captured, even if there's only one record -- else a tiny table is
        #    unfindable.)
        # 3. After all data, write the index region: per captured pair emit
        #    [klen u32][offset u64][key]. Remember where the index region began
        #    (index_offset) and how many entries it has (index_count).
        # 4. Write the footer: _FOOTER.pack(index_offset, index_count, SST_MAGIC).
        # 5. self.disk.fsync().
        raise NotImplementedError
        # === END ===


# ---------------------------------------------------------------------------
# READER  (Exercise 2.1.2 is the point read inside get())
# ---------------------------------------------------------------------------

class SSTableReader:
    """Read an SSTable. Loads the sparse index on open; data stays on disk.

    `self._index` is the resident sparse index: (key, data_offset) pairs, sorted.
    Its length is the metric L2.1 is about -- it must be SUB-LINEAR in the number
    of records, unlike L1's one-entry-per-key hash index.
    """
    def __init__(self, disk: Disk) -> None:
        self.disk = disk
        self._index: list[tuple[bytes, int]] = []   # (key, data_offset), sorted
        self._data_end = 0                           # first byte of index region
        self._load_index()

    def _load_index(self) -> None:
        # Provided: parse the footer, then load the sparse index into RAM.
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
        SSTable does not contain the key at all.

        Note the return type: a Record that may be a tombstone, NOT Optional[bytes].
        "absent here" (None) and "present but deleted" (tombstone Record) are
        different answers -- a later merge across SSTables needs to tell them apart.
        """
        # === YOUR CODE (Exercise 2.1.2) ===
        # 1. Binary-search self._index for the largest indexed key <= `key`.
        #    That gives the data offset to start scanning from. If `key` is
        #    smaller than every indexed key, it can't be here -> return None.
        # 2. Scan forward record-by-record from that offset (read the block
        #    self.disk.read(start, self._data_end - start) once and walk it with
        #    decode_record_at) until:
        #       - rec.key == key  -> return rec  (tombstone or not)
        #       - rec.key  > key  -> not present -> return None
        #       - you reach self._data_end -> return None
        raise NotImplementedError
        # === END ===

    def __iter__(self) -> Iterator[Record]:
        """Yield all records in sorted order (used by scans and, later, compaction)."""
        block = self.disk.read(0, self._data_end)
        off = 0
        while off < len(block):
            rec, off = decode_record_at(block, off)
            yield rec
