from __future__ import annotations

from harness import SimClock, SimDisk
from lsmstore import LSMStore, make_factory


def new_store(store=None, max_bytes=4096):
    store = {} if store is None else store
    return LSMStore(make_factory(store), SimClock(), max_bytes=max_bytes), store


def test_flush_then_read():
    """A key that lives ONLY in a flushed SSTable is still found."""
    db, _ = new_store()
    db.put(b"alpha", b"1")
    db.flush()                       # alpha now lives only in sst-0
    assert b"alpha" not in db._mem    # really gone from memory
    # TODO (write the oracle): get still returns it from the SSTable
    raise NotImplementedError("fill in the assertion")


def test_value_shadowing():
    """Newer memtable value wins over the same key's older SSTable value."""
    db, _ = new_store()
    db.put(b"k", b"old")
    db.flush()                       # old -> sst-0
    db.put(b"k", b"new")             # new -> memtable
    # TODO (write the oracle): newest wins
    raise NotImplementedError("fill in the assertion")


def test_delete_shadowing():
    """A tombstone in the memtable hides an older SSTable value -> None."""
    db, _ = new_store()
    db.put(b"k", b"v")
    db.flush()                       # v -> sst-0
    db.delete(b"k")                  # tombstone -> memtable
    # TODO (write the oracle): tombstone shadows the SSTable value
    raise NotImplementedError("fill in the assertion")


def test_crash_before_flush_wal_replay():
    """Write keys, crash BEFORE flush, reopen, assert writes survive via WAL.

    'Crash before flush' is a precise instant: flush is the durability boundary.
    Here every put is fsync'd to the WAL but nothing is flushed, so the SSTables
    are empty and recovery depends entirely on WAL replay.
    """
    disks = {}
    db, _ = new_store(disks, max_bytes=10**9)   # huge -> no auto-flush
    db.put(b"a", b"1")
    db.put(b"b", b"2")
    # CRASH: drop db, keep the durable bytes in `disks`.
    del db
    db2, _ = new_store(disks, max_bytes=10**9)  # reopen over the same disks
    # TODO (write the oracle): both writes present, reconstructed from the WAL
    raise NotImplementedError("fill in the assertions")


def test_crash_after_flush_truncates_wal():
    """After flush, the SSTable holds the data and the WAL is empty; recovery
    still works (now from the SSTable, not the WAL)."""
    disks = {}
    db, _ = new_store(disks, max_bytes=10**9)
    db.put(b"a", b"1")
    db.flush()
    assert disks["wal"].size() == 0             # WAL truncated on flush
    del db
    db2, _ = new_store(disks, max_bytes=10**9)
    # TODO (write the oracle): still recoverable, now from the SSTable
    raise NotImplementedError("fill in the assertion")
