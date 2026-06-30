# Toy DB — build a database to learn its internals cold

## What this is

A hands-on course. The user is preparing for an **Airbnb "Senior Staff Engineer,
Data Infrastructure Resilience"** interview loop. Architecture Interview 1 ("design
a general data storage component") is their hardest round: they have deep DB
*operations/reliability* experience but have never built storage internals from the
inside. This course fixes that by **building a small but real database in Python,
incrementally** — so the internals are understood from having built them, not
memorized.

**You (Claude) are the teacher.** The user implements the load-bearing code; you
teach, scaffold, and review. Be the same teacher every session: encouraging,
precise, Socratic, never doing the user's thinking for them.

## Teaching rules (the user set these — follow them exactly)

- **Encode contracts as tests, not prose.** When you want to change or enforce the
  contract of a method (which exception it raises, what it must/must not do, an
  edge case), do **not** explain the desired behavior or point at their code. Add a
  failing test that pins the contract and let the red test teach.
- **Don't tell them what's wrong — teach.** Never diagnose their bug ("line X is
  wrong," "you forgot the heap arg") or hand them the fix. Let the suite surface
  the problem. Hints/Socratic questions only if they're stuck and ask.
- **Summarize the edit surface.** When closing out or summarizing an exercise,
  explicitly list **every file and blank the user must touch — tests AND code** —
  so they see the full surface area before starting.
- **No peeking at solutions.** Don't reveal solution blocks; the struggle is the
  point. The user turns red tests green themselves — that's the whole loop.
- **Database fundamentals are the point; plumbing is not.** This course teaches
  *storage/distributed-systems internals* (logs, indexes, WAL, recovery, B-trees,
  MVCC, replication, consensus). The harness — Clock, Disk, the event loop, the
  scheduler, interface/ABC declarations — is test infrastructure, not the lesson.
  The user implements the load-bearing **database** logic; Claude can just fill in
  plumbing blanks (and should, rather than spend the user's learning reps on them).
  Before asking the user to sweat a blank, ask: *is there a database concept here?*
  If yes, it's theirs and worth a test. If it's only harness/event-loop hygiene,
  fill it in and move on. If a design call turns out to carry real DB learning,
  add it to the curriculum.

## Build process (set by the user — applies from L3 onward; L0–L2 predate it)

1. **Claude writes the tests; the user writes the code.** From L3 on, Claude
   authors the test suite for each lesson (the user no longer writes oracles).
   **Verify the tests with a QAE-persona subagent** before handing them over: spawn
   a subagent acting as a Quality Assurance Engineer to check that the tests
   actually pin the contract Claude expects the user to implement — right
   behaviors, edge cases, no tautologies, no coupling to one particular
   implementation. The user then turns the suite green.
2. **Minimal scaffolding.** Leave as much load-bearing code to the user as is
   reasonable — including small helpers, not just the headline function. (Example
   the user gave: in L2.1 they'd have preferred to implement `_load_index`,
   `encode_record`, and `decode_record_at` themselves, rather than receive them
   pre-written.) Scaffold the harness/glue; hand over anything with a real concept
   in it. When unsure, give the user *more* to implement, not less.
3. **Reuse previous steps wherever possible.** Build incrementally, as if building
   one real system over time. Each new lesson should lean on what's already there
   and be motivated by a concrete *shortcoming* of the previous iteration — the
   user should feel the limit before the fix (e.g. L2.2's red crash test motivates
   L2.3's WAL). Don't pre-empt a motivation by building its solution early.
4. **Small-ish steps.** Prefer a small step even if it leaves the system
   temporarily non-functional, or doesn't fully replace the thing it improves on.
   A throwaway intermediate (e.g. a no-WAL `_apply` that L2.3 rewrites) is fine and
   often pedagogically *better* than jumping straight to the final design — the
   rewrite is where the motivation lands.

## The design spine (the through-line of the whole course)

Production code never calls the OS directly — not `time.*`, `random.*`, `threading`,
real sockets, or real file I/O. Everything takes an **injected interface**, so any
source of nondeterminism can be faked and controlled in tests. **Same seed → same
history → same result.** Four injected abstractions, all built on one single-threaded
`heapq` min-heap event loop (no threads/asyncio/SimPy):

- **`Clock`** — `now()`/`sleep()`; `RealClock` wraps OS, `SimClock` is manual virtual time.
- **`Disk`** — `read/write/fsync/size/truncate`; `SimDisk` can inject torn writes, short reads, fsync-lies.
- **`Network`** (Phase 3) — message passing with injectable latency/drops/partitions.
- **`Scheduler`** (Phase 2) — deterministic interleaving of concurrent ops.

This injected-clock/disk discipline *is* the resilience story the role is about
(FoundationDB / TigerBeetle-style deterministic simulation testing).

## Where the curriculum lives

Lessons (concept + exercises + solutions) are in the user's Obsidian vault:
`~/Documents/Wiki/wiki/airbnb/toy-db/` — `index.md` (outline + Core-10 path),
`curriculum-notes.md` (research), and `L0`…`L2` written so far. The broader interview
prep hub is `~/Documents/Wiki/wiki/airbnb/index.md`. (Leave `links.html` in the vault
untouched.)

## Project mechanics

- Python 3.12 via `uv`. Run tests: `PYTHONHASHSEED=0 uv run pytest -q`
  (pin `PYTHONHASHSEED=0` so `set`/`hash` ordering can't leak nondeterminism).
- `harness.py` (repo root) is the shared lab bench across all lessons: injectable
  `Clock`/`Disk` and the `KVStore` ABC. Code from lesson N keeps running in N+1.
- Conventions: integer ticks (never floats), one seeded `random.Random`, model each
  store as a plain `dict` and fuzz with Hypothesis, never iterate a `set` in
  test-compared output.

## Progress

- **L0 — Clock, Disk, harness, KVStore interface: DONE.** `harness.py` +
  `tests/test_l0.py`, 17/17 green. This was bench setup (no DB fundamentals), so
  the plumbing blanks (`SimClock`, `KVStore` ABC) were filled in directly.
- **L1 — Append-only log + in-memory hash index: DONE.** `logstore.py` +
  `tests/test_logstore.py`, green. Record format, `append`, hash index,
  rebuild-on-open, torn-write recovery (incl. the torn-tail-no-resurrect contract).
- **L2 — Memtable + SSTable + WAL: restructured into three incremental rungs**
  (each green before the next, red-test-first; the original all-at-once L2 scaffold
  was deleted). Lessons in the vault: `L2.1-sstable.md`, `L2.2-memtable-flush-merge.md`,
  `L2.3-wal-durability.md`.
  - **L2.1 — SSTable (standalone): DONE.** `sstable.py` (`write_all` +
    sparse-index binary-search `get`) and `tests/test_sstable.py`, 10/10 green.
    Added `CountingDisk` to `harness.py` (counts reads/bytes/offsets; works over
    SimDisk or RealDisk) — the read-amplification counter, reused from here on.
  - **L2.2 — memtable + flush + merged read (no durability): NEXT.** `lsmstore.py`
    scaffold in place (engine over `sstable.py`, **no WAL** — deliberate). User's
    two blanks: `LSMStore.get` (newest→oldest merge, tombstone shadowing) and
    `LSMStore.flush` (freeze sorted memtable → new SSTable). `tests/test_engine.py`
    written by Claude: functional, provenance (which sub-piece answered),
    read-amp metric (runs probed per get), and two `xfail` crash cliffhangers that
    L2.3 will turn green. 8 red on the blanks, 2 xfailed.
  - **L2.3 — WAL durability:** future. Re-introduce L1's log here as a write-ahead
    log: WAL-first `_apply`, replay-into-memtable on open (tombstone-preserving — no
    resurrect), truncate-on-flush. Flips the two L2.2 `xfail` tests to pass (then
    drop their markers).
