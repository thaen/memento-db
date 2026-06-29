from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from harness import SimDisk
from logstore import LogStore


def fresh_disk() -> SimDisk:
    """An empty in-memory disk."""
    return SimDisk()


def test_basic_put_get():
    store = LogStore.open(fresh_disk())
    store.put(b"alice", b"1")
    store.put(b"bob", b"2")
    assert store.get(b"alice") == b"1"
    assert store.get(b"bob") == b"2"
    assert store.get(b"carol") is None


def test_empty_value_is_not_a_delete():
    # The flag, not vlen==0, marks a delete. An empty value must round-trip.
    store = LogStore.open(fresh_disk())
    store.put(b"k", b"")
    assert store.get(b"k") == b""          # present, empty — NOT None
    store.delete(b"k")
    assert store.get(b"k") is None         # now actually gone


def test_reopen_and_recover():
    disk = fresh_disk()
    store = LogStore.open(disk)
    keys = {f"key{i}".encode(): f"val{i}".encode() for i in range(50)}
    for k, v in keys.items():
        store.put(k, v)
    store.close()

    # Reopen on the SAME disk bytes: index must be rebuilt from the file.
    reopened = LogStore.open(disk)
    for k, v in keys.items():
        assert reopened.get(k) == v


def test_tombstone_survives_reopen():
    disk = fresh_disk()
    store = LogStore.open(disk)
    store.put(b"k", b"v")
    store.delete(b"k")
    store.close()

    reopened = LogStore.open(disk)
    assert reopened.get(b"k") == None


def test_last_write_wins():
    disk = fresh_disk()
    store = LogStore.open(disk)
    for v in [b"v1", b"v2", b"v3", b"final"]:
        store.put(b"k", v)
    assert store.get(b"k") == b"final"
    reopened = LogStore.open(disk)          # and after recovery too
    assert reopened.get(b"k") == b"final"


# ---- Hypothesis round-trip property -------------------------------------
# Model the store as a plain dict. For any sequence of puts/deletes, after a
# reopen the store must match the model exactly. This is how real storage
# engines are fuzzed.

keys = st.binary(min_size=0, max_size=8)
values = st.binary(min_size=0, max_size=16)
# An op is ("put", k, v) or ("delete", k)
ops = st.lists(
    st.one_of(
        st.tuples(st.just("put"), keys, values),
        st.tuples(st.just("delete"), keys, st.just(b"")),
    ),
    max_size=100,
)


@settings(max_examples=200)
@given(ops)
def test_roundtrip_matches_model(op_list):
    disk = fresh_disk()
    store = LogStore.open(disk)
    model: dict[bytes, bytes] = {}
    for op in op_list:
        if op[0] == "put":
            _, k, v = op
            store.put(k, v)
            model[k] = v
        else:
            _, k, _ = op
            store.delete(k)
            model.pop(k, None)
    store.close()

    reopened = LogStore.open(disk)
    # TODO: after reopen, the store must match the model for every key that
    # was ever touched (present keys equal, deleted/absent keys are None).
    for k in keys_seen(op_list):
        assert reopened.get(k) == model.get(k)


def keys_seen(op_list):
    return {op[1] for op in op_list}


# ---- The torn-write test: the injected Disk earns its keep ---------------
# Simulate a crash mid-append by truncating the last record's bytes. Recovery
# must drop the partial trailing record WITHOUT crashing, and every complete
# prior record must still be readable.

def test_torn_trailing_record_is_dropped():
    disk = fresh_disk()
    store = LogStore.open(disk)
    store.put(b"good1", b"aaa")
    store.put(b"good2", b"bbb")
    # This put is the one that "crashes" mid-write.
    store.put(b"partial", b"cccccccccc")
    store.close()

    full_size = disk.size()
    # Chop off the last few bytes of the final record — a torn write. We pick
    # a truncation point INSIDE the last record but past the others.
    disk.truncate(full_size - 4)

    reopened = LogStore.open(disk)          # must not raise
    assert reopened.get(b"good1") == b"aaa"
    assert reopened.get(b"good2") == b"bbb"
    assert reopened.get(b"partial") == None

    # Oddities about truncation:
    # 1. There's no test for truncation that occurs of the key, rather than the value.
    # 2. It's not clear what to do if an error in the log is discovered -- probably we need to
    #    copy the entire log into a new file without the new record? I can't think of another way
    #    to resolve the problem without modifying the log file itself.

def test_torn_tail_does_not_resurrect_after_later_writes():
    # Recovery must repair the LOG, not just the in-memory index. After a torn
    # trailing write is dropped, the store keeps serving traffic and appends a
    # new record. If recovery left the torn bytes in the file, the next append
    # lands on top of them and a later reopen mis-scans the torn record's stale
    # header -- resurrecting the dead key and/or corrupting the new one.
    #
    # Contract: once recovery drops a torn record, it stays dropped for all
    # time, and records appended afterward are read back exactly.
    disk = fresh_disk()
    store = LogStore.open(disk)
    store.put(b"keep", b"value")
    store.put(b"victim", b"0123456789")
    store.close()

    disk.truncate(disk.size() - 4)          # crash mid-write of victim

    recovered = LogStore.open(disk)
    assert recovered.get(b"victim") is None  # dropped on first recovery
    recovered.put(b"newkey", b"Z")           # DB keeps running, appends normally
    recovered.close()

    reopened = LogStore.open(disk)           # crash again, recover again
    assert reopened.get(b"victim") is None   # must STAY dead, not resurrect
    assert reopened.get(b"newkey") == b"Z"   # the later write survives intact
    assert reopened.get(b"keep") == b"value"


@pytest.mark.parametrize("chop", range(1, 12))
def test_torn_at_every_byte_offset(chop):
    # Stronger: truncating the last record at ANY byte boundary must leave the
    # prior records intact and never crash. Deterministic because SimDisk is a
    # plain bytearray — we can crash at byte N exactly.
    disk = fresh_disk()
    store = LogStore.open(disk)
    store.put(b"keep", b"value")
    store.put(b"victim", b"0123456789")
    store.close()

    disk.truncate(disk.size() - chop)
    reopened = LogStore.open(disk)          # never raises, for any chop
    assert reopened.get(b"keep") == b"value"
