from __future__ import annotations

import threading

import pytest
from hypothesis import given, strategies as st

from harness import CountingDisk, SimClock, SimDisk
from lsmstore import LSMStore, make_factory


# Large default so nothing auto-flushes mid-test unless we call flush() ourselves.
def new_store(max_bytes: int = 1 << 30):
    store: dict = {}
    return LSMStore(make_factory(store), SimClock(), max_bytes=max_bytes), store


def counting_store(max_bytes: int = 1 << 30):
    """Like new_store but every backing disk is a CountingDisk, so tests can assert
    on how many runs a get() actually probed (read amplification)."""
    backing: dict = {}

    def factory(name: str) -> CountingDisk:
        if name not in backing:
            backing[name] = CountingDisk(SimDisk())
        return backing[name]

    return LSMStore(factory, SimClock(), max_bytes=max_bytes), backing


# ---------------------------------------------------------------------------
# Functional: the store behaves like a key/value map across memtable + flushes.
# ---------------------------------------------------------------------------

def test_put_get_delete_roundtrip():
    db, _ = new_store()
    db.put(b"a", b"1")
    db.put(b"b", b"2")
    assert db.get(b"a") == b"1"
    assert db.get(b"b") == b"2"
    assert db.get(b"missing") is None
    db.delete(b"a")
    assert db.get(b"a") is None


def test_overwrite_takes_the_newest_value_across_a_flush():
    db, _ = new_store()
    db.put(b"k", b"old")
    db.flush()                 # old is now in sst-0
    db.put(b"k", b"new")       # new is in the memtable
    assert db.get(b"k") == b"new"
    db.flush()                 # new is now in sst-1 (newer run)
    assert db.get(b"k") == b"new"


# ---------------------------------------------------------------------------
# Provenance: prove WHICH sub-piece answered, not just that the value is right.
# ---------------------------------------------------------------------------

def test_flush_moves_the_key_from_memtable_to_an_sstable():
    db, _ = new_store()
    db.put(b"k", b"v")
    assert b"k" in db._mem                       # before flush: lives in RAM
    db.flush()
    # proof by elimination: the memtable can no longer be the source ...
    assert b"k" not in db._mem
    assert db._mem == {}
    # ... and direct inspection: the newest SSTable holds it.
    rec = db._ssts[-1].get(b"k")
    assert rec is not None and rec.value == b"v"
    assert db.get(b"k") == b"v"                   # the merged read still finds it


def test_value_shadowing_keeps_both_copies_and_get_chooses_newest():
    db, _ = new_store()
    db.put(b"k", b"old")
    db.flush()                                   # old -> sst-0
    db.put(b"k", b"new")                          # new -> memtable
    # both copies COEXIST: the old one is still physically in the SSTable ...
    assert db._ssts[-1].get(b"k").value == b"old"
    assert db._mem[b"k"].value == b"new"
    # ... and the merge actively CHOSE the newer one (not "old happened to be gone").
    assert db.get(b"k") == b"new"


def test_delete_shadowing_tombstone_hides_a_live_older_value():
    db, _ = new_store()
    db.put(b"k", b"v")
    db.flush()                                   # live value -> sst-0
    db.delete(b"k")                              # tombstone -> memtable
    # the live old value is STILL in the SSTable (delete erases nothing on disk) ...
    sst_rec = db._ssts[-1].get(b"k")
    assert sst_rec is not None and not sst_rec.tombstone and sst_rec.value == b"v"
    # ... but the memtable tombstone shadows it, so the merged read says "gone".
    assert db._mem[b"k"].tombstone
    assert db.get(b"k") is None


# ---------------------------------------------------------------------------
# The metric: read amplification = how many runs a get() probes.
# ---------------------------------------------------------------------------

def test_memtable_hit_probes_zero_sstables():
    db, _ = counting_store()
    db.put(b"x", b"0")
    db.flush()                                   # one SSTable exists ...
    db.put(b"x", b"1")                           # ... but x is fresh in the memtable
    for sst in db._ssts:
        sst.disk.reset()
    assert db.get(b"x") == b"1"
    probed = sum(1 for sst in db._ssts if sst.disk.stats.reads > 0)
    assert probed == 0                           # memtable answered; no SSTable touched


def test_key_in_the_oldest_run_probes_every_run():
    """A key present only in the OLDEST of K runs forces get() to probe all K
    (newest->oldest, first hit wins). Each run brackets the target key (lo < target
    < hi) so a miss still does a real data read -- making 'probed' disk-observable.
    This rising cost is the pain L3's compaction + Bloom filters will relieve."""
    K = 4
    db, _ = counting_store()
    for i in range(K):
        if i == 0:
            db.put(b"target", b"found")          # only in the oldest run (sst-0)
        db.put(b"aaa", b"lo")                     # lo < "target"
        db.put(b"zzz", b"hi")                     # hi > "target"
        db.flush()                               # -> one run per iteration
    assert len(db._ssts) == K

    for sst in db._ssts:
        sst.disk.reset()
    assert db.get(b"target") == b"found"
    probed = sum(1 for sst in db._ssts if sst.disk.stats.reads > 0)
    assert probed == K                           # had to touch every run to find it


# ---------------------------------------------------------------------------
# Concurrency: the write_lock guards flush, but the emptiness guard sits OUTSIDE it.
# ---------------------------------------------------------------------------

def test_two_racing_flushes_do_not_create_a_spurious_empty_run():
    """RED (regression): `flush()` checks `if not self._mem: return` OUTSIDE the
    write_lock, so two flushes can BOTH pass that guard while the memtable is
    non-empty, then serialize on the lock -- the first writes the run and resets the
    memtable, the second acquires the lock and writes an EMPTY run over the now-empty
    memtable, bumping _next_sst.

    The barrier forces exactly that interleaving deterministically (no timing / no
    SimClock needed): it lives in the lock's __enter__, which runs only AFTER flush()
    has passed the memtable guard, so reaching it proves both flushes saw memtable
    contents. Expected: one run. Buggy: two (the second is empty).

    Fix under test: re-check `if not self._mem: return` INSIDE the lock."""
    db, _ = new_store()
    db.put(b"k", b"v")

    real_lock = db.write_lock
    both_past_guard = threading.Barrier(2)

    class GatedLock:
        def __enter__(self):
            both_past_guard.wait()   # both flushes are past the memtable guard
            real_lock.acquire()
            return self

        def __exit__(self, *exc):
            real_lock.release()

    db.write_lock = GatedLock()

    threads = [threading.Thread(target=db.flush) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(db._ssts) == 1, f"a second, empty run was flushed: {len(db._ssts)} runs"
    assert db.get(b"k") == b"v"


def test_put_that_crosses_max_bytes_auto_flushes_without_hanging():
    """RED (regression): _apply() auto-calls flush() when the memtable crosses
    max_bytes. If put() and flush() both take the SAME non-reentrant write_lock,
    a threshold-crossing put self-deadlocks (put holds the lock, flush waits for
    it -- same thread, forever). The other engine tests miss this because they use
    a huge max_bytes, so the auto-flush branch never runs.

    Run the put on a daemon thread; a deadlock shows up as a join timeout rather
    than hanging the whole suite."""
    db, _ = new_store(max_bytes=1)            # any non-empty put crosses the line
    finished = threading.Event()

    def do_put():
        db.put(b"k", b"v")                    # -> _apply -> flush (both want the lock)
        finished.set()

    threading.Thread(target=do_put, daemon=True).start()
    assert finished.wait(timeout=2), "put() that auto-flushes deadlocked on write_lock"
    assert db.get(b"k") == b"v"
    assert len(db._ssts) == 1                 # the auto-flush actually produced a run


def test_put_during_flush_does_not_corrupt_the_run_being_written():
    """RED (race): put()/_apply never takes write_lock, so a put can mutate
    self._mem while flush() is iterating it (`sorted(self._mem)`) to build the run
    -> RuntimeError: dictionary changed size during iteration.

    Deterministic without relying on timing: a gate is installed INSIDE the
    memtable's iterator (not the lock). flush() yields its first key, signals, then
    blocks until a concurrent put() has mutated the live dict, then resumes -- the
    exact instant CPython raises. (A gate in write_lock, like the racing-flush test,
    can't reach this: the offending put doesn't take the lock.)

    Fix under test: flush must FREEZE the memtable so concurrent puts can't perturb
    the dict being iterated -- e.g. swap in a fresh memtable under the lock and
    write the detached copy, and/or make _apply take write_lock too. NOTE: any fix
    that makes the interleaved put wait for the flush is correct; this test then
    spends up to `gate_timeout` letting that put block, which is expected, not a
    failure."""
    gate_timeout = 1.0
    db, _ = new_store()
    db.put(b"k0", b"v0")
    db.put(b"k1", b"v1")

    flush_reached_iteration = threading.Event()
    put_completed = threading.Event()

    def gated(base_iter):
        it = iter(base_iter)
        yield next(it)                        # one item out ...
        flush_reached_iteration.set()         # ... we're mid-iteration
        put_completed.wait(timeout=gate_timeout)  # ... let a put mutate the dict
        yield from it                         # resume: faults if the dict grew

    class GatedMem(dict):
        # cover whichever the impl iterates; this flush uses __iter__ (sorted(dict)).
        def __iter__(self):    return gated(dict.__iter__(self))
        def keys(self):        return gated(dict.keys(self))
        def values(self):      return gated(dict.values(self))
        def items(self):       return gated(dict.items(self))

    db._mem = GatedMem(db._mem)

    crash: list[BaseException] = []

    def do_flush():
        try:
            db.flush()
        except BaseException as e:            # the racing thread's RuntimeError
            crash.append(e)

    flusher = threading.Thread(target=do_flush)
    flusher.start()
    assert flush_reached_iteration.wait(timeout=5), "flush never iterated the memtable"

    db.put(b"k2", b"v2")                       # interleaved write into the memtable
    put_completed.set()
    flusher.join(timeout=5)

    assert not crash, f"flush crashed on a concurrent put: {crash[0]!r}"
    assert db.get(b"k0") == b"v0"              # nothing the flush froze was lost
    assert db.get(b"k1") == b"v1"
    assert db.get(b"k2") == b"v2"              # ... nor the interleaved write


# ---------------------------------------------------------------------------
# The cliffhanger: NO durability yet. These are RED on purpose -- they are the
# precise reason L2.3 (the WAL) exists. Marked xfail so the suite stays green;
# L2.3 will make them pass and we'll drop the markers.
# ---------------------------------------------------------------------------

@pytest.mark.xfail(reason="L2.2 has no durability; the WAL arrives in L2.3", strict=True)
def test_crash_before_flush_loses_unflushed_writes():
    store: dict = {}
    db = LSMStore(make_factory(store), SimClock())
    db.put(b"k1", b"v1")
    db.put(b"k2", b"v2")
    # crash: drop the engine, KEEP the disks, reopen over the same store.
    db2 = LSMStore(make_factory(store), SimClock())
    assert db2.get(b"k1") == b"v1"
    assert db2.get(b"k2") == b"v2"


@pytest.mark.xfail(reason="L2.2 has no durability; the WAL arrives in L2.3", strict=True)
def test_crash_after_flush_then_delete_resurrects_the_key():
    store: dict = {}
    db = LSMStore(make_factory(store), SimClock())
    db.put(b"k", b"v")
    db.flush()                                   # k is durable in sst-0
    db.delete(b"k")                              # tombstone only in the memtable
    # crash before the next flush: the tombstone was never made durable.
    db2 = LSMStore(make_factory(store), SimClock())
    assert db2.get(b"k") is None                 # but k resurrects from sst-0


# ---------------------------------------------------------------------------
# Property: the engine matches a plain dict model under random put/delete/flush.
# ---------------------------------------------------------------------------

keys = st.binary(min_size=1, max_size=4)
values = st.binary(min_size=0, max_size=6)
Op = st.one_of(
    st.tuples(st.just("put"), keys, values),
    st.tuples(st.just("delete"), keys, st.just(b"")),
    st.tuples(st.just("flush"), st.just(b""), st.just(b"")),
)


@given(ops=st.lists(Op, max_size=60))
def test_engine_matches_a_dict_model(ops):
    db, _ = new_store()
    model: dict[bytes, bytes] = {}
    for kind, k, v in ops:
        if kind == "put":
            db.put(k, v)
            model[k] = v
        elif kind == "delete":
            db.delete(k)
            model.pop(k, None)
        else:
            db.flush()
        # spot-check the key just touched plus a fixed probe key each step.
        for probe in (k, b"\x00"):
            assert db.get(probe) == model.get(probe)
