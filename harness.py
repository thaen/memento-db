"""The lab bench: injectable Clock, Disk, and the KVStore interface.

Nothing in production code ever calls time.*, random.*, threading, real sockets,
or real file I/O directly. Everything takes one of these injected interfaces.
Same seed -> same history -> same result.
"""

from __future__ import annotations

import abc
import heapq
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, Protocol, runtime_checkable

DEBUG = True
def pml(msg):
    if DEBUG:
        print(msg)

# ===========================================================================
# Clock
# ===========================================================================


@runtime_checkable
class Clock(Protocol):
    """Integer ticks = microseconds, monotonic."""

    def now(self) -> int:
        raise NotImplemented()
    def sleep(self, ticks: int) -> None: ...


class RealClock:
    """Wraps the OS clock. Provided complete -- do not modify."""

    def now(self) -> int:
        # monotonic_ns avoids wall-clock jumps (NTP, DST); // 1000 -> microseconds.
        return time.monotonic_ns() // 1000

    def sleep(self, ticks: int) -> None:
        if ticks < 0:
            raise ValueError("cannot sleep negative ticks")
        time.sleep(ticks / 1e6)


class SimClock:
    """Manually advanced virtual clock. Time is a single integer you control.

    Carries a min-heap of scheduled callbacks for the Phase-2 scheduler. The
    heap is present from L0 so later lessons need no refactor; we don't *drive*
    callbacks until Phase 2.
    """

    def __init__(self, start: int = 0) -> None:
        if not isinstance(start, int):
            raise TypeError("ticks must be int, never float")
        self._now: int = start
        # min-heap of (tick, seq, callback); seq breaks ties deterministically.
        self._heap: list[tuple[int, int, Callable[[], None]]] = []
        self._seq: int = 0

    def now(self) -> int:
        return self._now

    def advance(self, ticks: int) -> None:
        """Move virtual time forward by `ticks`. Must reject negatives and
        floats; time is monotonic and integer."""
        # Pure time movement: validate, then set. The event loop (Phase 2) is
        # the ONLY thing that pops and fires scheduled callbacks -- advance
        # never does. Type before value: wrong type -> TypeError, bad value -> ValueError.
        if not isinstance(ticks, int):
            raise TypeError("ticks must be int, never float")
        if ticks < 0:
            raise ValueError("cannot advance by negative ticks")
        self._now += ticks

    def sleep(self, ticks: int) -> None:
        """In the simulator, sleeping is just advancing virtual time. (In
        Phase 2 this will yield to the scheduler; for now, advance.)"""
        # === YOUR CODE (Lesson 0.1b): implement SimClock.sleep ===
        self.advance(ticks)
        # === END ===

    def schedule(self, at_tick: int, callback: Callable[[], None]) -> None:
        """Register `callback` to fire at `at_tick`. Pushes onto the min-heap,
        breaking ties by an increasing sequence number so equal-tick events run
        in insertion order. Used from Phase 2 on; just build the heap push now."""
        # === YOUR CODE (Lesson 0.1c): implement SimClock.schedule ===
        heapq.heappush(self._heap, (at_tick, self._seq, callback))
        self._seq += 1
        # === END ===


# ===========================================================================
# Disk  (provided complete -- read it, fill nothing in for L0)
# ===========================================================================


@runtime_checkable
class Disk(Protocol):
    """Byte-addressable file abstraction."""

    def read(self, offset: int, length: int) -> bytes: ...
    def write(self, offset: int, data: bytes) -> None: ...
    def fsync(self) -> None: ...
    def size(self) -> int: ...
    def truncate(self, size: int) -> None: ...
    def close(self) -> None: ...


class RealDisk:
    """Wraps a real file via os-level pread/pwrite. Provided complete."""

    def __init__(self, path: str) -> None:
        # O_CREAT so a fresh DB just works; not O_TRUNC so reopen preserves data.
        self._fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)

    def read(self, offset: int, length: int) -> bytes:
        return os.pread(self._fd, length, offset)

    def write(self, offset: int, data: bytes) -> None:
        n = os.pwrite(self._fd, data, offset)
        if n != len(data):
            raise IOError(f"short write: {n} of {len(data)} bytes")

    def fsync(self) -> None:
        # Note: on macOS plain fsync does not flush to platter; that needs
        # F_FULLFSYNC. Irrelevant for SimDisk (we model durability explicitly),
        # but a sharp real-world detail worth knowing for L5.
        os.fsync(self._fd)

    def size(self) -> int:
        return os.fstat(self._fd).st_size

    def truncate(self, size: int) -> None:
        os.ftruncate(self._fd, size)

    def close(self) -> None:
        os.close(self._fd)


class SimDisk:
    """In-memory disk backed by a bytearray.

    Fault-injection hooks are PRESENT but INERT in L0. L1 (torn writes) and L5
    (short reads, fsync-lies) flip them on without changing this interface.
    """

    def __init__(self) -> None:
        self._buf = bytearray()
        # --- fault-injection knobs, inert until a later lesson sets them ---
        self.tear_next_write_at: int | None = None   # L1: truncate a write
        self.short_read: bool = False                 # L5: return fewer bytes
        self.fsync_lies: bool = False                 # L5: drop unsynced writes

    def read(self, offset: int, length: int) -> bytes:
        end = offset + length
        data = bytes(self._buf[offset:end])
        if self.short_read and len(data) > 0:
            data = data[: len(data) - 1]  # inert unless short_read flipped on
        return data

    def write(self, offset: int, data: bytes) -> None:
        if self.tear_next_write_at is not None:
            data = data[: self.tear_next_write_at]   # inert unless set
            self.tear_next_write_at = None
        end = offset + len(data)
        if end > len(self._buf):
            self._buf.extend(b"\x00" * (end - len(self._buf)))
        self._buf[offset:end] = data

    def fsync(self) -> None:
        # Inert in L0/L1. L5 will model "fsync_lies" by tracking an unsynced
        # high-water mark and discarding past it on a simulated crash.
        return None

    def size(self) -> int:
        return len(self._buf)

    def truncate(self, size: int) -> None:
        if size < len(self._buf):
            del self._buf[size:]
        else:
            self._buf.extend(b"\x00" * (size - len(self._buf)))

    def close(self) -> None:
        return None


@dataclass
class DiskStats:
    """I/O tally for one CountingDisk. Reset between measurements."""
    reads: int = 0
    read_bytes: int = 0
    read_offsets: list[int] = field(default_factory=list)  # offset of each read()
    writes: int = 0
    write_bytes: int = 0
    fsyncs: int = 0


class CountingDisk:
    """Wraps ANY Disk and tallies its I/O, then delegates. Conforms to the Disk
    protocol, so it drops in wherever a Disk is expected -- SimDisk or RealDisk.

    Use it in tests to assert on *access patterns*, which functional assertions
    can't see: that a point read touches a bounded slice near the target instead
    of scanning the whole file (L2.1), or how many runs a get probes (L2.2's read
    amplification). Call `reset()` right before the operation you want to measure.
    """

    def __init__(self, inner: Disk) -> None:
        self._inner = inner
        self.stats = DiskStats()

    def reset(self) -> None:
        self.stats = DiskStats()

    def read(self, offset: int, length: int) -> bytes:
        data = self._inner.read(offset, length)
        self.stats.reads += 1
        self.stats.read_bytes += len(data)
        self.stats.read_offsets.append(offset)
        return data

    def write(self, offset: int, data: bytes) -> None:
        self._inner.write(offset, data)
        self.stats.writes += 1
        self.stats.write_bytes += len(data)

    def fsync(self) -> None:
        self.stats.fsyncs += 1
        self._inner.fsync()

    def size(self) -> int:
        return self._inner.size()

    def truncate(self, size: int) -> None:
        self._inner.truncate(size)

    def close(self) -> None:
        self._inner.close()


# ===========================================================================
# KVStore  (your interface to commit to)
# ===========================================================================


class KVStore(abc.ABC):
    """Ordered key/value store. Keys and values are bytes.

    You implement this five ways across the course. Commit to the contract:
      - get returns None for a missing/deleted key.
      - put overwrites.
      - delete is idempotent (deleting a missing key is fine).
      - scan yields (key, value) pairs with start <= key < end, in key order.
      - close releases resources.
    """

    @abc.abstractmethod
    def get(self, key: bytes) -> bytes | None:
        """Return the value for `key`, or None if missing/deleted."""
        ...

    @abc.abstractmethod
    def put(self, key: bytes, value: bytes) -> None:
        """Insert or overwrite `key` with `value`."""
        ...

    @abc.abstractmethod
    def delete(self, key: bytes) -> None:
        """Remove `key`. Idempotent: deleting a missing key is fine."""
        ...

    @abc.abstractmethod
    def scan(self, start: bytes, end: bytes) -> Iterator[tuple[bytes, bytes]]:
        """Yield (key, value) pairs with start <= key < end, in key order."""
        ...

    @abc.abstractmethod
    def close(self) -> None:
        """Release resources."""
        ...

