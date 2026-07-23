package com.zenith;

import com.zenith.raft.rpc.AppendEntriesRequest.LogEntryDTO;
import com.zenith.raft.state.RaftLog;
import com.zenith.raft.state.RaftState;
import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrent-access test suite for the lock-free / CAS-backed components:
 * MemoryEngine's ConcurrentHashMap-based storage, the idempotency LRU cache
 * (see the LinkedHashSet -> LinkedHashMap fix), and RaftLog's synchronized
 * mutation methods.
 *
 * Each test hammers real shared state from many threads and asserts a
 * concrete correctness property afterward — not just "it didn't throw".
 */
public class ConcurrentAccessTest {

    private static final int THREADS = 16;

    private ExecutorService pool() {
        return Executors.newFixedThreadPool(THREADS);
    }

    private void runConcurrently(int taskCount, java.util.function.IntConsumer task) throws Exception {
        ExecutorService pool = pool();
        CountDownLatch ready = new CountDownLatch(taskCount);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(taskCount);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            final int idx = i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    task.accept(idx);
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    done.countDown();
                }
            });
        }
        ready.await(5, TimeUnit.SECONDS);
        go.countDown(); // release all threads at once — maximize actual contention
        assertTrue(done.await(30, TimeUnit.SECONDS), "concurrent tasks did not finish in time");
        pool.shutdown();

        if (!errors.isEmpty()) {
            throw new AssertionError("concurrent task(s) threw: " + errors.get(0), errors.get(0));
        }
    }

    // ── MemoryEngine: primary/secondary index correctness under load ──

    @Test
    void concurrentInsertsAllVisibleAndCountCorrect() throws Exception {
        MemoryEngine engine = new MemoryEngine();
        int perThread = 200;
        runConcurrently(THREADS, threadIdx -> {
            for (int i = 0; i < perThread; i++) {
                engine.insertTrade(new Trade("T-" + threadIdx + "-" + i, "AAPL", 10, 100.0, "PENDING"));
            }
        });
        assertEquals(THREADS * perThread, engine.size(),
                "every concurrently-inserted trade must be visible — no lost updates");
    }

    @Test
    void concurrentInsertsAcrossDifferentThreadsAreAllIndividuallyRetrievable() throws Exception {
        MemoryEngine engine = new MemoryEngine();
        int perThread = 100;
        runConcurrently(THREADS, threadIdx -> {
            for (int i = 0; i < perThread; i++) {
                engine.insertTrade(new Trade("T-" + threadIdx + "-" + i, "MSFT", 5, 50.0, "PENDING"));
            }
        });
        for (int t = 0; t < THREADS; t++) {
            for (int i = 0; i < perThread; i++) {
                assertNotNull(engine.getTrade("T-" + t + "-" + i),
                        "trade T-" + t + "-" + i + " should be retrievable after concurrent insert");
            }
        }
    }

    @Test
    void concurrentTickerIndexStaysConsistentWithPrimaryIndex() throws Exception {
        MemoryEngine engine = new MemoryEngine();
        int perThread = 150;
        runConcurrently(THREADS, threadIdx -> {
            for (int i = 0; i < perThread; i++) {
                engine.insertTrade(new Trade("T-" + threadIdx + "-" + i, "GOOG", 1, 1.0, "PENDING"));
            }
        });
        List<Trade> byTicker = engine.getTradesByTicker("GOOG");
        assertEquals(THREADS * perThread, byTicker.size(),
                "secondary (ticker) index must stay in sync with primary index under concurrent writes");
    }

    @Test
    void concurrentDeletesAndInsertsProduceCorrectFinalCount() throws Exception {
        MemoryEngine engine = new MemoryEngine();
        int n = 500;
        for (int i = 0; i < n; i++) engine.insertTrade(new Trade("D-" + i, "TSLA", 1, 1.0, "PENDING"));

        // Half the threads delete even-indexed trades, half insert new ones — concurrently.
        runConcurrently(THREADS, threadIdx -> {
            if (threadIdx % 2 == 0) {
                for (int i = 0; i < n; i += 2) engine.deleteTrade("D-" + i);
            } else {
                for (int i = 0; i < 50; i++) {
                    engine.insertTrade(new Trade("NEW-" + threadIdx + "-" + i, "TSLA", 1, 1.0, "PENDING"));
                }
            }
        });

        // Every even-indexed original trade should be gone, every odd-indexed one should remain.
        for (int i = 0; i < n; i += 2) assertNull(engine.getTrade("D-" + i), "D-" + i + " should have been deleted");
        for (int i = 1; i < n; i += 2) assertNotNull(engine.getTrade("D-" + i), "D-" + i + " should still exist");
    }

    @Test
    void concurrentReadOfActiveTradesDuringWritesNeverThrows() throws Exception {
        // This directly exercises the exact hazard a comment in WriteAheadLog
        // warns about: iterating a live view while another thread mutates it
        // (ConcurrentModificationException). getAllActiveTrades() is supposed
        // to return a safe snapshot copy specifically to avoid this.
        MemoryEngine engine = new MemoryEngine();
        for (int i = 0; i < 200; i++) engine.insertTrade(new Trade("S-" + i, "NFLX", 1, 1.0, "PENDING"));

        AtomicInteger snapshotFailures = new AtomicInteger(0);
        runConcurrently(THREADS, threadIdx -> {
            if (threadIdx % 2 == 0) {
                for (int i = 0; i < 100; i++) {
                    try {
                        engine.getAllActiveTrades().size(); // must not throw while others mutate
                    } catch (Exception e) {
                        snapshotFailures.incrementAndGet();
                    }
                }
            } else {
                for (int i = 0; i < 50; i++) {
                    engine.insertTrade(new Trade("S-new-" + threadIdx + "-" + i, "NFLX", 1, 1.0, "PENDING"));
                }
            }
        });
        assertEquals(0, snapshotFailures.get(),
                "getAllActiveTrades() must return a safe snapshot even while concurrent writes are happening");
    }

    // ── Idempotency LRU cache under concurrent load ──

    @Test
    void idempotencyCacheStaysCappedUnderConcurrentLoad() throws Exception {
        // Directly exercises the LinkedHashSet -> LinkedHashMap fix: hammer
        // the cache with far more than 100k unique request IDs from many
        // threads and confirm eviction actually happened (previously it
        // silently never did).
        MemoryEngine engine = new MemoryEngine();
        int perThread = 10_000; // 16 threads * 10k = 160k unique ids, over the 100k cap
        runConcurrently(THREADS, threadIdx -> {
            for (int i = 0; i < perThread; i++) {
                engine.markProcessed("req-" + threadIdx + "-" + i);
            }
        });
        // We can't reach the internal Set directly (encapsulated), but we can
        // prove eviction happened: an early request from thread 0 should no
        // longer be remembered once well over 100k newer ones pushed it out.
        assertFalse(engine.hasProcessed("req-0-0"),
                "the very first inserted requestId should have been evicted once the 100k cap was exceeded " +
                        "by a wide margin — if this fails, the LRU eviction bug has regressed");
    }

    @Test
    void idempotencyCacheConsistentlyDetectsKnownDuplicateUnderConcurrentReads() throws Exception {
        MemoryEngine engine = new MemoryEngine();
        engine.markProcessed("known-request-id");

        AtomicInteger inconsistentReads = new AtomicInteger(0);
        runConcurrently(THREADS, threadIdx -> {
            for (int i = 0; i < 1000; i++) {
                if (!engine.hasProcessed("known-request-id")) {
                    inconsistentReads.incrementAndGet();
                }
            }
        });
        assertEquals(0, inconsistentReads.get(),
                "a known-processed requestId must be consistently reported as processed under concurrent reads");
    }

    // ── RaftLog: synchronized mutation correctness under concurrent appends ──

    @Test
    void raftLogConcurrentAppendsLoseNoEntries() throws Exception {
        RaftLog log = new RaftLog();
        int perThread = 50;
        AtomicInteger counter = new AtomicInteger(0);

        runConcurrently(THREADS, threadIdx -> {
            for (int i = 0; i < perThread; i++) {
                int idx = counter.getAndIncrement();
                log.appendEntry(new LogEntryDTO(idx, 1, "K" + idx, "V" + idx, "INSERT", (long) idx));
            }
        });

        assertEquals(THREADS * perThread, log.getLastLogIndex() + 1,
                "every concurrently-appended log entry must be present — no lost writes under synchronized appendEntry()");
    }

    @Test
    void raftStateConcurrentCandidateTransitionsIncrementTermExactlyOncePerCall() throws Exception {
        // Exercises AtomicInteger/CAS correctness directly: term increments
        // must never be lost even when many threads race to become candidate
        // at once (this can't happen in the real single-threaded RaftNode
        // inbox, but the underlying AtomicInteger guarantee should hold
        // regardless of caller — that's the whole point of using CAS here).
        RaftState state = new RaftState();
        runConcurrently(THREADS, threadIdx -> {
            for (int i = 0; i < 50; i++) {
                state.convertToCandidate();
            }
        });
        assertEquals(THREADS * 50, state.getCurrentTerm(),
                "AtomicInteger-backed term increments must never be lost under concurrent access");
    }

    // ── Property-based fuzz test: randomized concurrent operation mix ──

    @RepeatedTest(20)
    void randomizedConcurrentOperationMixNeverCorruptsEngine() throws Exception {
        MemoryEngine engine = new MemoryEngine();
        int seedTrades = 100;
        for (int i = 0; i < seedTrades; i++) engine.insertTrade(new Trade("R-" + i, "AMZN", 1, 1.0, "PENDING"));

        runConcurrently(8, threadIdx -> {
            java.util.Random r = new java.util.Random(threadIdx);
            for (int i = 0; i < 200; i++) {
                int op = r.nextInt(3);
                String id = "R-" + r.nextInt(seedTrades);
                switch (op) {
                    case 0 -> engine.getTrade(id);
                    case 1 -> engine.updateTradeStatus(id, "FILLED");
                    case 2 -> engine.getTradesByTicker("AMZN");
                }
            }
        });

        // No crash/exception occurred (runConcurrently would have thrown),
        // and the engine's basic invariant — size never negative, never
        // exceeds what was seeded — still holds.
        assertTrue(engine.size() >= 0 && engine.size() <= seedTrades,
                "engine size must stay within valid bounds after a randomized concurrent operation mix");
    }
}
