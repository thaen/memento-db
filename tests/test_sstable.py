from __future__ import annotations

from harness import CountingDisk, SimDisk
from sstable import Record, SPARSE_EVERY, SSTableReader, SSTableWriter, decode_record_at


def build(disk: SimDisk, records: list[Record]) -> SSTableReader:
    """Write `records` (any order) to `disk` as an SSTable, return a reader.
    write_all requires sorted input, so we sort here (a flush would do this)."""
    SSTableWriter(disk).write_all(sorted(records, key=lambda r: r.key))
    return SSTableReader(disk)


def test_writer_lays_out_sorted_data_and_a_strided_sparse_index():
    """Pin write_all directly (not just through get): the DATA region round-trips
    every record IN SORTED ORDER, and the INDEX region holds exactly the strided
    records -- positions 0, SPARSE_EVERY, 2*SPARSE_EVERY, ... -- and nothing else."""
    n = 10  # spans a few strides at SPARSE_EVERY=4 -> expect indexed positions 0,4,8
    records = [Record(f"key{i:02d}".encode(), f"val{i}".encode()) for i in range(n)]
    disk = SimDisk()
    reader = build(disk, records)  # build() sorts before writing

    sorted_records = sorted(records, key=lambda r: r.key)
    sorted_keys = [r.key for r in sorted_records]
    round_tripped = list(reader)          # __iter__ walks the DATA region
    indexed_keys = [k for (k, _off) in reader._index]
    expected_indexed = [sorted_keys[i] for i in range(0, n, SPARSE_EVERY)]

    # DATA region: every record comes back, byte-for-byte, in sorted-key order.
    assert round_tripped == sorted_records

    # INDEX region: exactly the strided records (#0, #4, #8) -- "0 always, then
    # every SPARSE_EVERY" -- and the offsets actually point at those records.
    assert indexed_keys == expected_indexed
    for key, off in reader._index:
        rec, _next = decode_record_at(reader.disk.read(off, reader._data_end - off), 0)
        assert rec.key == key

def test_binary_search():
    tr = SSTableReader(SimDisk())
    tr._index = [(b"alpha", 1),
                 (b"beta", 3),
                 (b"charlie", 5),
                 (b"delta", 7),
                 (b"echo", 9)]
    assert tr.floor_index_for_key(b"aardvark") == None
    assert tr.floor_index_for_key(b"charlie") == 2
    assert tr.floor_index_for_key(b"cohort") == 2
    assert tr.floor_index_for_key(b"dance") == 2
    assert tr.floor_index_for_key(b"extant") == 4
    assert tr.floor_index_for_key(b"zillion") == 4

    tr = SSTableReader(SimDisk())
    tr._index = [(b"alpha", 1),
                 (b"beta", 3)]
    assert tr.floor_index_for_key(b"aardvark") == None
    assert tr.floor_index_for_key(b"alpha") == 0
    assert tr.floor_index_for_key(b"avatar") == 0
    assert tr.floor_index_for_key(b"beta") == 1
    assert tr.floor_index_for_key(b"zeta") == 1

def test_all_keys_resolve_with_a_sparse_index():
    """The point of L2.1: an SSTable answers N lookups while keeping FAR FEWER
    than N index entries resident -- unlike L1's one-entry-per-key hash index."""
    n = 12
    records = [Record(f"key{i:02d}".encode(), f"val{i}".encode()) for i in range(n)]
    disk = SimDisk()
    reader = build(disk, records)

    # Every key resolves -- including the ones NOT in the sparse index (those force
    # a forward scan from the preceding indexed key).
    for r in records:
        got = reader.get(r.key)
        assert got is not None and got.value == r.value and not got.tombstone

    # A key that was never inserted resolves to None (here it sorts above all keys).
    assert reader.get(b"key99") is None

    # The metric: far fewer index entries resident than records. At SPARSE_EVERY=4,
    # n=12 -> indexed positions 0,4,8 -> 3 entries, not 12.
    expected_entries = len(range(0, n, SPARSE_EVERY))
    assert len(reader._index) == expected_entries
    assert len(reader._index) < n


def test_get_uses_the_index_not_a_full_scan():
    """Behavioral, not functional: prove get() BINARY-SEARCHES the sparse index and
    reads a bounded slice near the target -- it does NOT scan the whole data region
    from offset 0. A linear get() passes every other test in this file; only the
    I/O counter catches it.

    Query key10, which lives in the LAST sparse block (indexed at key08, offset > 0):
      - binary-search get(): first read starts at key08's offset, reads ~one block.
      - linear get():        first read starts at offset 0, reads the whole region.
    """
    n = 12  # sparse blocks 0-3, 4-7, 8-11; indexed positions 0,4,8
    records = [Record(f"key{i:02d}".encode(), f"val{i}".encode()) for i in range(n)]
    disk = CountingDisk(SimDisk())
    SSTableWriter(disk).write_all(sorted(records, key=lambda r: r.key))
    reader = SSTableReader(disk)        # _load_index() reads footer + index region

    block_offset = dict(reader._index)[b"key08"]   # where the last block starts
    data_region_bytes = reader._data_end           # size of the whole DATA region

    disk.reset()                        # measure ONLY the get() below
    got = reader.get(b"key10")          # non-indexed key in the last block
    assert got is not None and got.value == b"val10"

    # Used the index, didn't scan from 0 -- two complementary signals on disk.stats:
    # (1) every data read started AT or AFTER the key08 block, never at offset 0.
    assert disk.stats.read_offsets, "get() did no disk read at all"
    assert min(disk.stats.read_offsets) >= block_offset
    # (2) total bytes read are a bounded slice, not the whole data region
    #     (a linear get() reads exactly data_region_bytes starting from offset 0).
    assert disk.stats.read_bytes < data_region_bytes


def test_non_indexed_key_resolves_by_scanning_forward():
    """The sparse-index mechanic in isolation: a key that has NO index entry of its
    own (it sits between two indexed keys) is still found by scanning forward from
    the largest indexed key <= it."""
    n = 8  # indexed positions 0,4 -> keys key00,key04; key06 is non-indexed
    records = [Record(f"key{i:02d}".encode(), f"val{i}".encode()) for i in range(n)]
    disk = SimDisk()
    reader = build(disk, records)

    assert [k for (k, _off) in reader._index] == [b"key00", b"key04"]  # key06 NOT indexed
    got = reader.get(b"key06")
    assert got is not None and got.value == b"val6"


def test_tombstone_reads_back_as_a_tombstone_not_none():
    """The whole reason get() returns Optional[Record] and not Optional[bytes]:
    a deleted key is PRESENT-but-tombstoned here, which must be distinguishable from
    absent (None) -- a later cross-SSTable merge depends on telling them apart."""
    disk = SimDisk()
    reader = build(disk, [
        Record(b"alive", b"v"),
        Record(b"dead", b"", tombstone=True),
    ])

    dead = reader.get(b"dead")
    assert dead is not None            # present...
    assert dead.tombstone and dead.value == b""   # ...but tombstoned, not a value

    alive = reader.get(b"alive")
    assert alive is not None and not alive.tombstone and alive.value == b"v"

    assert reader.get(b"never") is None   # truly absent -> None, the other answer


def test_single_record_table_is_findable():
    """Edge: even a 1-record table must resolve -- pins 'record #0 is always indexed'."""
    disk = SimDisk()
    reader = build(disk, [Record(b"solo", b"x")])

    got = reader.get(b"solo")
    assert got is not None and got.value == b"x"
    assert reader.get(b"other") is None


def test_key_below_everything_returns_none():
    """Edge: a key smaller than every key in the file (binary-search underflow --
    no indexed key <= target, so there's nothing to scan)."""
    disk = SimDisk()
    reader = build(disk, [Record(b"m", b"1"), Record(b"p", b"2"), Record(b"t", b"3")])
    assert reader.get(b"a") is None


def test_key_above_everything_returns_none():
    """Edge: a key larger than every key (the forward scan runs off the end of the
    data region into the index region and must stop -- the symmetric boundary to
    the underflow case above)."""
    disk = SimDisk()
    reader = build(disk, [Record(b"m", b"1"), Record(b"p", b"2"), Record(b"t", b"3")])
    assert reader.get(b"z") is None


def test_missing_key_in_a_gap_returns_none():
    """Edge: a key that sorts strictly between two present keys. The forward scan
    must stop early when it sees a key > target (not run to the end)."""
    disk = SimDisk()
    reader = build(disk, [Record(b"a", b"1"), Record(b"c", b"2"), Record(b"e", b"3")])
    assert reader.get(b"d") is None
    assert reader.get(b"b") is None


def test_missing_key_in_a_full_non_last_block_returns_none():
    """RED (regression): a missing key that floors into a NON-LAST sparse block that
    is EXACTLY full (SPARSE_EVERY records) must return None, not crash.

    With SPARSE_EVERY=4, keys a..h index at 'a' (offset 0) and 'e'. 'cc' floors into
    the first block [a,b,c,d]; after decoding 'd' the scan offset equals the block
    length exactly. The current scan (`off > len(chunk)`, no `rec.key > key` stop,
    last-block chunk sized by disk.size()) decodes one record past the block and
    raises struct.error here. The gap/above tests above miss it because their tiny
    tables land in the LAST block, where the alignment happens not to fault."""
    disk = SimDisk()
    reader = build(disk, [Record(bytes([c]), b"v") for c in b"abcdefgh"])
    assert [k for k, _ in reader._index] == [b"a", b"e"]   # 'cc' floors into block #0
    assert reader.get(b"cc") is None
