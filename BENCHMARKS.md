# ZenithDB — Real Benchmarks

Every number in this document was actually measured — either by me against a
real 3-node Docker cluster on real hardware (Yashwant's machine), or by
Claude in a sandboxed environment with a note on exactly what was and
wasn't verified there. Nothing here is estimated or assumed. Where a
number turned out to be disappointing, it's reported as-is, with the root
cause explained — that's more valuable in an interview than a suspiciously
perfect number.

---

## 1. Functional correctness — verified live, zero errors

`ZenithLiveDemo` (Phase 1) against a real running 3-node cluster:

```
INSERT,T001,AAPL,100,185.50,PENDING,REQ-001  → PENDING: Trade submitted to Raft Consensus Cluster.
SELECT,T001                                   → FOUND: T001 | AAPL | Qty: 100 | $185.5 | EXECUTED
UPDATE,T001,EXECUTED,UPD-001                   → PENDING: Trade submitted to Raft Consensus Cluster.
SELECT_TICKER,AAPL                             → FOUND 1 TRADES: [T001 | $185.5 | EXECUTED]
DELETE,T002                                    → PENDING: Trade submitted to Raft Consensus Cluster.
SELECT,T002                                    → NOT_FOUND: Trade T002 does not exist.
```
Zero `ERROR:` responses. Insert, read, update, ticker lookup, and delete
all correctly round-tripped through real Raft consensus.

## 2. Automated test suite — 57/57 passing, real JUnit5/Surefire run

```
Tests run: 57, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
Total time: 07:10 min
```
Covers: leader election, split-brain/partition tolerance (isolated
minority never wins, majority partition still elects, partition heals and
reconverges, restart doesn't cause double-voting), and concurrent-access
correctness (16 threads hammering storage/idempotency/log simultaneously).
Run with `mvn test`.

## 3. Live crash recovery — the whole story, proven, not simulated

`docker compose stop node-b` → `docker compose start node-b`, real logs:

```
WAL recovery complete — replayed 4 entries (2 with idempotency history restored)
♻️ Restored persisted Raft state: term=2, votedFor=node-a:9001
♻️ Restored persisted Raft log: 5 entries, baseIndex=0, lastApplied=4
```
Followed by all three nodes independently computing the **identical**
anti-entropy hash (`3d9102004cdd86e0...`) — proof node-b's data was
byte-for-byte in sync with the other two after recovery, not just
"probably fine."

This single restart demonstrates three separate fixes built during this
project, all working together: idempotency history surviving restart,
split-brain-safe term/vote persistence, and full Raft log persistence.

## 4. Idempotency — proven under real duplicate traffic

```
♻️ [IDEMPOTENCY node-b:9002] Blocked duplicate: REQ-001
```
A genuine duplicate request, live, correctly rejected exactly once.

## 5. WAL compaction — real measured size reduction

5,000 inserts + 3,000 updates, then compacted:
```
Before: 315,760 bytes (8,000 log lines)
After:  139,990 bytes (5,000 active trades)
Reduction: 55.7%
```
**Honest caveat:** this is workload-dependent — a churnier workload (more
repeated updates per trade before settling) would push this higher. Report
the real number for whatever workload you describe, or re-run this
benchmark with a workload closer to your actual use case.

## 6. Docker image size — real, replaces a fabricated number

```
zenith-db-node-a   210MB
zenith-db-node-b   210MB
zenith-db-node-c   210MB
```
(A `-alpine` JRE base was already in use; 210MB reflects the fat JAR plus
JVM plus Jackson — not the "50MB" previously and incorrectly claimed with
no benchmark behind it.)

## 7. Throughput under real concurrent load — the honest, harder number

100 concurrent users, 100,000 total operations across 5 phases (bulk
insert, high-volume select, concurrent update/CAS stress, idempotency
stress, realistic mixed workload):

```
Total operations : 100,000
Successes         : 3,225
Failures          : 31,590
Overall throughput: 23 ops/sec
Peak throughput    : 38 ops/sec
```

**This is real, and it's a genuine finding, not noise.** Two real bugs in
the load-test tooling itself were found and fixed during this project
(Phase 2 sending reads to a follower instead of the leader; one slow
response cascading into hundreds of false failures) — but even after both
fixes, throughput under 100 concurrent users stays in the tens-to-low-
hundreds of ops/sec range, well short of the previously-claimed
"10,000+ ops/sec," which had no benchmark behind it at all.

**Root cause:** every write requires synchronous, full Raft consensus (a
network round-trip to both peers, plus a WAL flush) processed through a
**single-threaded NIO event loop** handling all client and peer connections.
Under 100 simultaneous connections all issuing consensus-requiring
commands, that single thread becomes the bottleneck — later operations
queue up, some exceed socket timeouts, and the leader can even lose its
lease and trigger unnecessary re-elections under sustained contention
(visible in Grafana's "Leader Elections Over Time" panel spiking during
this test).

**This is genuinely good interview material, not a weakness to hide:**
*"I load-tested at 100 concurrent users and found the single-threaded NIO
event loop becomes a bottleneck under synchronous per-write consensus —
here's what I measured, here's the root cause, and here's what I'd change
to fix it: move client command handling off the selector thread onto a
worker pool, and/or batch multiple client commands into a single
AppendEntries round instead of one round trip per write."*

---

## What to say if asked "what's your system's real throughput?"

Be direct: *"At low-to-moderate concurrency, every operation I tested
completed correctly with no errors. Under a 100-concurrent-user stress
test, I measured a real bottleneck — about 20-40 ops/sec sustained — and
traced it to the single-threaded network layer combined with synchronous
consensus per write. I know exactly why, and exactly what I'd change to
fix it."* That's a stronger answer than any unverified big number.

## What's still open, for full transparency
- No isolated, controlled measurement of average per-request latency in
  milliseconds exists yet in this document — the mechanism (`ZenithMetrics`
  latency tracking) works and is wired up correctly, it just hasn't been
  captured from a clean, low-concurrency run. To get this: run `ZenithLiveDemo`
  or a handful of manual commands, then immediately query
  `zenith_request_latency_ms` at `localhost:9090/graph` — that'll give a
  real number for a light-load scenario, separate from the heavy-load
  throughput numbers above.
- The single-threaded NIO bottleneck under high concurrency is documented,
  not fixed — a legitimate, scoped next step if this project continues.
