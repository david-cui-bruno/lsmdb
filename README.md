# lsmdb

A log-structured merge-tree storage engine in C++20 (~450 lines, header-only
core) with a durability contract that is actually enforced by a kill -9
crash harness: **every acknowledged write survives SIGKILL at any instant**,
including mid-flush and mid-compaction.

Companion to [raft-kv](https://github.com/david-cui-bruno/raft-kv): that
project verifies consensus above the storage line; this one builds and
verifies the storage engine below it.

```
 put/del ──► WAL (CRC per record, fsync) ──► memtable (sorted)
                                                 │ size threshold
                                                 ▼
                                          SSTable L0 (immutable)
                                                 │ L0 count threshold
                                                 ▼
                                       compaction ──► L1 (single run)
```

## The durability contract, and how it's tested

An acked write = `Put`/`Delete` returned true = WAL record appended + fsynced.
The crash harness (`tests/crash_test.cc`) forks a writer child that applies a
deterministic op stream (puts, overwrites of a hot key set, deletes) and acks
each op through a pipe only after it is durable. The parent:

1. SIGKILLs the child at a random moment (30-430 ms in),
2. reopens the database (recovery = MANIFEST load + WAL replay),
3. reconstructs the expected state for ops `0..last_acked` and verifies
   every key, allowing only the single in-flight op to differ.

**Result: 30/30 rounds clean, 179,008 acked ops, zero lost or corrupted
writes** (`results/crash.log`). The tiny memtable threshold (16 KB) forces
flushes and compactions to be in progress at kill time in nearly every round.

Torn-tail handling is also unit-tested directly: garbage appended to the WAL
is discarded by CRC, a flipped byte mid-record conservatively ends replay at
the corruption point, and the database stays writable after both.

## Crash-safety mechanics

- **WAL records** carry CRC32C over length+kind+payload; replay stops at the
  first bad record (anything after it was never acked).
- **SSTables** are written to a temp file, fsynced, then renamed; the
  directory is fsynced after rename (a crash leaves either the old or the
  new state, never a partial file in the MANIFEST).
- **MANIFEST** (the list of live SSTables) is replaced atomically the same
  way. Compaction commits the new manifest before deleting old files, so a
  crash between those steps leaks a file at worst, never loses data.
- **Recovery** is just: load MANIFEST ▸ open SSTables ▸ replay WAL tail.
  There is no "clean shutdown" path to depend on.

## Benchmarks (M5 Pro, 100-byte values, `results/bench.json`)

| operation | throughput | note |
|---|---|---|
| put, fsync per write | 42K ops/s | the honest durable-write cost |
| put, no fsync | 659K ops/s | group-commit ceiling |
| get from memtable | 6.9M ops/s (0.14 µs) | sorted map hit |
| get from L0/L1 SSTable | 151K ops/s (6.6 µs) | binary-searched index + one pread |
| WAL recovery | 2.9M records/s | 100K records replayed in 34 ms |

The fsync gap (16x) is why real engines group-commit; the L0/L1 read cost is
one `fopen`+`pread` per lookup, which a block cache would amortize. Both are
documented limitations, not surprises.

## Tests

```bash
make test    # 11 unit tests + 30-round crash harness
make bench   # benchmarks -> results/bench.json
```

Unit tests cover: roundtrip/overwrite, tombstones, flush + SSTable reads,
reopen from WAL only, reopen from SSTables + WAL, newest-level-wins across
memtable/L0/L1, compaction merging + tombstone dropping + reopen, automatic
compaction triggering, torn WAL tail, corrupt mid-WAL record, and 1 MB values
with binary keys.

## Limitations (deliberate scope)

Single-threaded, no block cache, no bloom filters, no range iterators, L0→L1
full-merge compaction only (no leveled tiers). Each of those is an extension
point, not a correctness gap; the crash harness is the part most projects
skip, so it came first.
