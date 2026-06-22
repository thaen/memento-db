from __future__ import annotations

import os

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from harness import Clock, RealClock, SimClock
from harness import Disk, RealDisk, SimDisk
from harness import KVStore


# --- Protocol conformance: both clocks satisfy the Clock contract ----------

def test_clocks_are_clocks():
    assert isinstance(RealClock(), Clock)
    assert isinstance(SimClock(), Clock)


def test_disks_are_disks(tmp_path):
    assert isinstance(SimDisk(), Disk)
    assert isinstance(RealDisk(str(tmp_path / "f.db")), Disk)


# --- Exercise 1: SimClock basics -------------------------------------------

def test_simclock_advance_and_now():
    c = SimClock(start=100)
    assert c.now() == 100
    c.advance(50)
    # === YOUR CODE (Lesson 0.1-test): assert now() is what you expect ===
    assert c.now() == 150
    # === END ===


def test_simclock_rejects_negative_and_float():
    c = SimClock()
    with pytest.raises(ValueError):
        c.advance(-1)
    with pytest.raises(TypeError):
        SimClock(start=1.5)  # ticks are ints, never floats


def test_simclock_sleep_advances():
    c = SimClock()
    c.sleep(7)
    assert c.now() == 7


# --- Exercise 2: interchangeability (results, not timing) ------------------

REQUESTED_SLEEPS = (3, 1, 4, 1, 5)


def workload(clock: Clock) -> list[int]:
    """A clock-agnostic workload. Returns the observed delta for each sleep:
    how far now() moved across each clock.sleep(d) call."""
    out: list[int] = []
    for d in REQUESTED_SLEEPS:
        before = clock.now()
        clock.sleep(d)
        out.append(clock.now() - before)
    return out


def test_simclock_workload_is_exact():
    # A deterministic clock yields the requested deltas exactly. That's the
    # whole point of SimClock: no overshoot, no noise.
    assert workload(SimClock()) == list(REQUESTED_SLEEPS)


def test_realclock_honors_the_clock_contract():
    # You CANNOT assert exact deltas against a real clock -- the OS overshoots
    # microsecond sleeps by a noisy amount. "Interchangeable" means both honor
    # the Clock CONTRACT, not that they print identical numbers. The contract:
    #   - sleep(d) waits AT LEAST d (never returns early)
    #   - time is monotonic (now() never goes backwards)
    deltas = workload(RealClock())
    for requested, observed in zip(REQUESTED_SLEEPS, deltas):
        assert observed >= requested          # never sleeps less than asked
    c = RealClock()
    prev = c.now()
    for d in REQUESTED_SLEEPS:
        c.sleep(d)
        assert c.now() >= prev                # monotonic
        prev = c.now()


# --- Exercise 3: Hypothesis monotonicity property --------------------------

@given(
    start=st.integers(min_value=0, max_value=10_000),
    advances=st.lists(st.integers(min_value=0, max_value=1_000), max_size=50),
)
@settings(max_examples=200)
def test_simclock_monotonic(start: int, advances: list[int]):
    c = SimClock(start=start)
    prev = c.now()
    for d in advances:
        c.advance(d)
        # now() is non-decreasing after every advance
        assert c.now() >= prev
        prev = c.now()
    # final now() equals start + sum of all advances
    # === YOUR CODE (Lesson 0.3-test): assert the sum invariant ===
    assert c.now() == start + sum(advances)
    # === END ===


# --- Exercise 1c: schedule builds a deterministic heap ---------------------

def test_schedule_orders_by_tick_then_insertion():
    c = SimClock()
    fired: list[str] = []
    c.schedule(10, lambda: fired.append("b"))
    c.schedule(10, lambda: fired.append("a"))  # same tick, scheduled later
    c.schedule(5, lambda: fired.append("first"))
    # Drain the heap by tick, then seq. (We're not running the loop yet; we
    # just verify the heap pops in the right order.)
    order = []
    while c._heap:
        tick, seq, cb = __import__("heapq").heappop(c._heap)
        cb()
        order.append(tick)
    assert fired == ["first", "b", "a"]  # 5 first; same-tick keeps insert order
    assert order == [5, 10, 10]


# --- Exercise 4: KVStore is abstract ---------------------------------------

def test_kvstore_is_abstract():
    with pytest.raises(TypeError):
        KVStore()  # cannot instantiate an ABC with abstract methods


# --- Contract: advance/sleep input validation ------------------------------

def test_advance_rejects_float():
    c = SimClock()
    with pytest.raises(TypeError):
        c.advance(1.5)


def test_advance_rejects_non_numeric():
    c = SimClock()
    with pytest.raises(TypeError):
        c.advance("nope")


def test_advance_rejects_negative_float():
    # A value that is BOTH the wrong type (float) AND a bad value (negative).
    # The type is wrong, so this is a TypeError -- floats are banned regardless
    # of sign. Which check fires first decides whether you get this right.
    c = SimClock()
    with pytest.raises(TypeError):
        c.advance(-1.5)


def test_sleep_rejects_negative():
    c = SimClock()
    with pytest.raises(ValueError):
        c.sleep(-1)


# --- Contract: in L0, advancing time does NOT fire scheduled callbacks ------
# The event loop that drives scheduled callbacks is built in Phase 2. In L0,
# `schedule` only records the callback on the heap; `advance` just moves time.

def test_advance_does_not_fire_callbacks_in_l0():
    c = SimClock()
    fired: list[str] = []
    c.schedule(5, lambda: fired.append("x"))
    c.schedule(8, lambda: fired.append("y"))
    c.advance(10)
    assert fired == []
    assert c.now() == 10
    # the callbacks are still sitting on the heap, untouched
    assert len(c._heap) == 2


# --- Contract: during a fired callback, now() reads the firing tick ---------
# Pins the behavior for when Phase 2 *does* drive the loop: a callback scheduled
# for tick T must observe clock.now() == T while it runs (not the pre-advance
# time). We assert it by draining the heap the way the loop eventually will.

def test_now_equals_firing_tick_when_loop_drives_callbacks():
    c = SimClock()
    seen: list[int] = []

    def record():
        seen.append(c.now())

    c.schedule(5, record)
    c.schedule(12, record)

    # Minimal stand-in for the Phase-2 loop: pop in heap order, set now to the
    # event's tick, then run it.
    import heapq
    while c._heap:
        tick, seq, cb = heapq.heappop(c._heap)
        c._now = tick
        cb()
    assert seen == [5, 12]


# --- Disk round-trips faithfully (faults inert) ----------------------------

def test_simdisk_roundtrip():
    d = SimDisk()
    d.write(0, b"hello")
    d.write(5, b"world")
    assert d.read(0, 10) == b"helloworld"
    assert d.size() == 10
    d.truncate(5)
    assert d.read(0, 10) == b"hello"
