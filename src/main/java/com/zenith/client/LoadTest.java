package com.zenith.client;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * ZenithDB Load Test — verifies 10,000+ ops/sec throughput
 *
 * WHAT THIS TESTS:
 *   Phase 1 — INSERT throughput    (write path: NIO → Raft → MemoryEngine → WAL)
 *   Phase 2 — SELECT throughput    (read path:  NIO → MemoryEngine direct, no Raft)
 *   Phase 3 — UPDATE throughput    (CAS update: NIO → Raft → ConcurrentHashMap.replace())
 *   Phase 4 — DELETE throughput    (tombstone:  NIO → Raft → MemoryEngine + WAL)
 *   Phase 5 — Mixed workload       (70% read, 20% insert, 10% update — realistic trading)
 *
 * HOW TO RUN (after docker-compose up):
 *   Option A — IntelliJ: Right-click this file → Run 'LoadTest.main()'
 *   Option B — Terminal: mvn exec:java -Dexec.mainClass="com.zenith.client.LoadTest"
 *   Option C — Against Docker: set TARGET_HOST = "localhost", TARGET_PORT = 9001
 *
 * EXPECTED RESULTS on a modern laptop:
 *   INSERT: 8,000 – 15,000 ops/sec  (WAL batching absorbs disk I/O cost)
 *   SELECT: 20,000 – 50,000 ops/sec (lock-free ConcurrentHashMap reads)
 *   UPDATE: 6,000 – 12,000 ops/sec  (CAS retry overhead under contention)
 *   DELETE: 7,000 – 13,000 ops/sec  (similar to INSERT — goes through Raft)
 *   MIXED:  10,000+ ops/sec combined
 */
public class LoadTest {

    // ── Configuration ──────────────────────────────────────────────────────
    private static final String TARGET_HOST  = "localhost";
    private static final int    TARGET_PORT  = 9001;      // always send to leader port
    private static final int    NUM_THREADS  = 20;        // concurrent client connections
    private static final int    OPS_PER_THREAD = 500;     // operations per thread per phase
    private static final int    TOTAL_OPS    = NUM_THREADS * OPS_PER_THREAD; // 10,000 total
    // ───────────────────────────────────────────────────────────────────────

    // Shared counters — AtomicLong so 20 threads can increment safely
    private static final AtomicLong successCount = new AtomicLong(0);
    private static final AtomicLong failureCount = new AtomicLong(0);
    private static final AtomicLong totalLatencyNs = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║       ZenithDB Load Test — 10,000 ops/sec       ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("Target  : " + TARGET_HOST + ":" + TARGET_PORT);
        System.out.println("Threads : " + NUM_THREADS);
        System.out.println("Ops/thread: " + OPS_PER_THREAD);
        System.out.println("Total ops : " + TOTAL_OPS + " per phase\n");

        // ── Verify connection before starting ──
        System.out.print("Checking connection to ZenithDB... ");
        try {
            Socket test = new Socket(TARGET_HOST, TARGET_PORT);
            test.close();
            System.out.println("✅ Connected\n");
        } catch (Exception e) {
            System.out.println("❌ FAILED");
            System.out.println("Make sure ZenithDB is running:");
            System.out.println("  Docker: docker-compose up");
            System.out.println("  Local:  Run Main.java first");
            return;
        }

        // Pre-generate trade IDs so SELECT/UPDATE/DELETE phases can find them
        List<String> insertedTradeIds = Collections.synchronizedList(new ArrayList<>());

        // ──────────────────────────────────────────────────────────────────
        // PHASE 1 — INSERT
        // ──────────────────────────────────────────────────────────────────
        System.out.println("━━━ PHASE 1: INSERT (" + TOTAL_OPS + " trades) ━━━");
        runPhase("INSERT", NUM_THREADS, OPS_PER_THREAD, (threadId, opIndex) -> {
            String tradeId  = "T" + String.format("%06d", threadId * OPS_PER_THREAD + opIndex);
            String ticker   = randomTicker();
            int    qty      = 100 + (opIndex % 900);
            double price    = 100.0 + (opIndex % 400) + 0.5;
            String reqId    = "REQ-" + tradeId;

            String cmd = String.format("INSERT,%s,%s,%d,%.2f,PENDING,%s",
                    tradeId, ticker, qty, price, reqId);
            insertedTradeIds.add(tradeId);
            return cmd;
        });

        Thread.sleep(1000); // let Raft commit propagate

        // ──────────────────────────────────────────────────────────────────
        // PHASE 2 — SELECT (read-only, bypasses Raft, hits MemoryEngine directly)
        // ──────────────────────────────────────────────────────────────────
        System.out.println("\n━━━ PHASE 2: SELECT (" + TOTAL_OPS + " reads) ━━━");
        runPhase("SELECT", NUM_THREADS, OPS_PER_THREAD, (threadId, opIndex) -> {
            // Pick a random existing trade to read
            if (insertedTradeIds.isEmpty()) return "SELECT,T000000";
            String tradeId = insertedTradeIds.get(
                    (threadId * OPS_PER_THREAD + opIndex) % insertedTradeIds.size());
            return "SELECT," + tradeId;
        });

        // ──────────────────────────────────────────────────────────────────
        // PHASE 3 — UPDATE (CAS-based status change through Raft)
        // ──────────────────────────────────────────────────────────────────
        System.out.println("\n━━━ PHASE 3: UPDATE (" + TOTAL_OPS + " updates) ━━━");
        String[] statuses = {"EXECUTED", "SETTLED", "CANCELLED", "PENDING"};
        runPhase("UPDATE", NUM_THREADS, OPS_PER_THREAD, (threadId, opIndex) -> {
            if (insertedTradeIds.isEmpty()) return null;
            String tradeId  = insertedTradeIds.get(
                    (threadId * OPS_PER_THREAD + opIndex) % insertedTradeIds.size());
            String newStatus = statuses[opIndex % statuses.length];
            String reqId     = "UPD-" + threadId + "-" + opIndex;
            return "UPDATE," + tradeId + "," + newStatus + "," + reqId;
        });

        Thread.sleep(500);

        // ──────────────────────────────────────────────────────────────────
        // PHASE 4 — DELETE (tombstone write through Raft + WAL)
        // ──────────────────────────────────────────────────────────────────
        System.out.println("\n━━━ PHASE 4: DELETE (" + TOTAL_OPS / 2 + " deletes) ━━━");
        // Only delete half so we still have data for mixed phase
        int deleteOps = OPS_PER_THREAD / 2;
        runPhase("DELETE", NUM_THREADS, deleteOps, (threadId, opIndex) -> {
            int idx = threadId * deleteOps + opIndex;
            if (idx >= insertedTradeIds.size()) return null;
            String tradeId = insertedTradeIds.get(idx);
            return "DELETE," + tradeId;
        });

        Thread.sleep(500);

        // ──────────────────────────────────────────────────────────────────
        // PHASE 5 — MIXED workload (most realistic — like a real trading system)
        // 70% SELECT, 20% INSERT, 10% UPDATE
        // ──────────────────────────────────────────────────────────────────
        System.out.println("\n━━━ PHASE 5: MIXED WORKLOAD (" + TOTAL_OPS + " ops) ━━━");
        System.out.println("    Distribution: 70% SELECT, 20% INSERT, 10% UPDATE");
        AtomicInteger mixedInsertCounter = new AtomicInteger(TOTAL_OPS);
        runPhase("MIXED", NUM_THREADS, OPS_PER_THREAD, (threadId, opIndex) -> {
            int roll = opIndex % 10;
            if (roll < 7) {
                // 70% — SELECT a random trade
                if (insertedTradeIds.isEmpty()) return null;
                String tradeId = insertedTradeIds.get(
                        (threadId * OPS_PER_THREAD + opIndex) % insertedTradeIds.size());
                return "SELECT," + tradeId;
            } else if (roll < 9) {
                // 20% — INSERT a new trade
                String tradeId = "MX" + String.format("%06d", mixedInsertCounter.getAndIncrement());
                insertedTradeIds.add(tradeId);
                return String.format("INSERT,%s,MSFT,200,350.00,PENDING,REQ-%s", tradeId, tradeId);
            } else {
                // 10% — UPDATE an existing trade
                if (insertedTradeIds.isEmpty()) return null;
                String tradeId = insertedTradeIds.get(
                        (threadId * OPS_PER_THREAD + opIndex) % insertedTradeIds.size());
                return "UPDATE," + tradeId + ",EXECUTED,MX-" + threadId + "-" + opIndex;
            }
        });

        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║                  TEST COMPLETE                  ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("Total trades in system: " + insertedTradeIds.size());
        System.out.println("\nTo verify data persisted, run:");
        System.out.println("  telnet localhost 9001");
        System.out.println("  SELECT," + (insertedTradeIds.isEmpty() ? "T000000" : insertedTradeIds.get(0)));
    }

    // ── Core runner — spins up NUM_THREADS threads, each running OPS_PER_THREAD ops ──
    private static void runPhase(String phaseName, int threads, int opsPerThread,
                                 CommandGenerator generator) throws InterruptedException {
        successCount.set(0);
        failureCount.set(0);
        totalLatencyNs.set(0);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        long phaseStart = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    // Each thread opens ONE persistent connection for all its ops
                    // This is realistic — connection pooling like a real client
                    try (Socket socket = new Socket(TARGET_HOST, TARGET_PORT);
                         PrintWriter out = new PrintWriter(
                                 new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                         BufferedReader in = new BufferedReader(
                                 new InputStreamReader(socket.getInputStream()))) {

                        socket.setSoTimeout(5000); // 5 second read timeout

                        for (int op = 0; op < opsPerThread; op++) {
                            String command = generator.generate(threadId, op);
                            if (command == null) continue; // skip null commands

                            long opStart = System.nanoTime();
                            out.println(command);

                            String response = in.readLine();
                            long opEnd = System.nanoTime();

                            totalLatencyNs.addAndGet(opEnd - opStart);

                            if (response != null && !response.startsWith("ERROR") &&
                                    !response.startsWith("❌")) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    failureCount.addAndGet(opsPerThread);
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(120, TimeUnit.SECONDS); // wait up to 2 minutes
        pool.shutdown();

        long phaseEnd = System.nanoTime();
        double durationSec = (phaseEnd - phaseStart) / 1_000_000_000.0;
        long totalOps = successCount.get() + failureCount.get();
        double opsPerSec = totalOps / durationSec;
        double avgLatencyMs = totalOps > 0
                ? (totalLatencyNs.get() / 1_000_000.0) / totalOps
                : 0;

        // ── Print results ──
        System.out.printf("  Duration    : %.2f seconds%n", durationSec);
        System.out.printf("  Total ops   : %,d%n", totalOps);
        System.out.printf("  ✅ Success  : %,d%n", successCount.get());
        System.out.printf("  ❌ Failures : %,d%n", failureCount.get());
        System.out.printf("  Throughput  : %,.0f ops/sec%n", opsPerSec);
        System.out.printf("  Avg latency : %.2f ms/op%n", avgLatencyMs);

        // ── Pass/fail verdict ──
        if (opsPerSec >= 10_000) {
            System.out.printf("  Result      : ✅ PASSED — %.0f ops/sec ≥ 10,000 target%n", opsPerSec);
        } else if (opsPerSec >= 5_000) {
            System.out.printf("  Result      : ⚠️  PARTIAL — %.0f ops/sec (network or Raft overhead)%n", opsPerSec);
        } else {
            System.out.printf("  Result      : ❌ FAILED — %.0f ops/sec (check if leader is up)%n", opsPerSec);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────
    private static final String[] TICKERS = {"AAPL", "GOOGL", "MSFT", "TSLA", "AMZN",
            "GS", "JPM", "BAC", "MS", "BLK"};

    private static String randomTicker() {
        return TICKERS[(int)(Math.random() * TICKERS.length)];
    }

    @FunctionalInterface
    interface CommandGenerator {
        String generate(int threadId, int opIndex) throws Exception;
    }
}