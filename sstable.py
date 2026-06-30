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

DEBUG = True
def pml(msg):
    if DEBUG:
        print(msg)

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
        # Lay the file down in three contiguous regions, in this exact byte order:
        #
        #   [ DATA region ] [ INDEX region ] [ FOOTER ]
        #   ^offset 0       ^index_offset    ^total - FOOTER_SIZE
        #
        # DATA region (starts at offset 0):
        #   For each record in input order, write encode_record(rec) -- i.e.
        #   [flags u8][klen u32][vlen u32][key][value] -- back to back, no padding.
        #   Record i's data_offset is the sum of the encoded lengths before it.
        #   (`records` is already sorted by key, so the file is physically sorted.)
        #
        # INDEX region (starts at index_offset = total bytes of the DATA region):
        #   A SPARSE index -- capture (key, data_offset) for the records at
        #   positions 0, SPARSE_EVERY, 2*SPARSE_EVERY, ...  Position 0 is ALWAYS
        #   captured, even for a one-record table (else a tiny table is unfindable).
        #   Write each captured pair as  _IDX.pack(len(key), data_offset) + key
        #   -- i.e. [klen u32][offset u64][key] -- back to back. index_count is the
        #   number of pairs written.  (_load_index reads them back in this format.)
        #
        # FOOTER (the last FOOTER_SIZE bytes of the file):
        #   _FOOTER.pack(index_offset, index_count, SST_MAGIC)
        #   -- i.e. [index_offset u64][index_count u64][MAGIC u32].
        #
        # Finally: self.disk.fsync().
        offset = 0
        # Our disk is just ours, no need to lock here.
        buffer = bytearray()
        idx = []
        offset = 0
        pml(f"write_all")
        for i, record in enumerate(records):
            pml(f" {i} writing {record}")
            record_bytes = encode_record(record)
            if (i + SPARSE_EVERY) % SPARSE_EVERY == 0:
                # we need to record this one.
                idx.append( (record.key, offset) )
            buffer.extend(record_bytes)
            # self.disk.write(offset, record_bytes)
            offset += len(record_bytes)

        idx_offset = offset
        pml(f"wrote data to offset {offset}, now creating index region.")
        
        for key, koffset in idx:
            idx_entry = bytearray()
            idx_entry.extend(_IDX.pack(len(key), koffset))
            idx_entry.extend(key)
            buffer.extend(idx_entry)
            offset += len(idx_entry)

        pml(f"buffer now contains the data plus index region, to offset {offset}. let's write the footer.")
        buffer.extend(_FOOTER.pack(idx_offset, len(idx), SST_MAGIC))

        pml(f"buffer now contains data, idx, footer, len {len(buffer)}. writing.")
        self.disk.write(0, bytes(buffer))
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
        if total == 0:
            return
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

    def floor_index_for_key(self, key: bytes) -> int:
        # given a key we know to be in the range of keys we might have,
        # find the offset to start scanning forward from.

        if len(self._index) < 3:
            # binary search unnecesary.
            # find the first index lower than the key
            for item in reversed(self._index):
                if key <= item[0]:
                    return item[1]

        # binary search now.
        left = 0
        right = len(self._index)
        pml(f"binary searching for {key} in self._index")
        while True:
            cur_idx = int((left + right) / 2)
            pml(f"  k: {key}; l: {left}; r: {right}; cur: {cur_idx}; len: {len(self._index)}")

            if left == right:
                return None
            
            if key < self._index[cur_idx][0]:
                # if the key is to the left, move the right side of the bounds.
                right = cur_idx
            else:
                # key is right of this entry, or is this entry.
                # if the next entry is off the end, or if it's greater than us, then we're good.
                if (key == self._index[cur_idx][0]
                    or cur_idx+1 >= len(self._index)
                    or key < self._index[cur_idx+1][0]):
                    return cur_idx

                # if we haven't stopped and key is right of where we are, then move the left pointer.
                left = cur_idx

        pml("something terrible has happened")
        raise Exception("halp")
            
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
        pml(f"eval'ing index {self._index} to see if we might have key {key}")
        if not self._index:
            return None
        if key < self._index[0][0]:
            # key is lower than we store.
            return None

        idx = self.floor_index_for_key(key)
        if idx == None:
            return None

        start_key, offset = self._index[idx]
        pml(f"will start scan at {start_key} at offset {offset}")
            
        # read this offset to the next one (or the end)
        if idx+1 >= len(self._index):
            chunk_size = self.disk.size() - offset
        else:
            chunk_size = self._index[idx+1][1] - offset
        pml(f"reading chunk from offset above of length {chunk_size}")
        chunk = self.disk.read(offset, chunk_size)
        pml(f"{chunk}")

        off = 0
        while True:
            if off > len(chunk):
                return None
            pml(f"read chunk offset {off}")
            record, off = decode_record_at(chunk, off)
            pml(f"found {record}")
            if record and record.key == key:
                return record
        return None
        
    def __iter__(self) -> Iterator[Record]:
        """Yield all records in sorted order (used by scans and, later, compaction)."""
        block = self.disk.read(0, self._data_end)
        off = 0
        while off < len(block):
            rec, off = decode_record_at(block, off)
            yield rec
