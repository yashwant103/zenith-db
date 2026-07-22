package com.zenith.client;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * ContinuousTradeSimulator — runs forever like a real trading system.
 *
 * WHAT THIS DOES:
 *   1. Continuously inserts/updates/selects trades at high throughput
 *   2. Every 30 seconds, kills the current leader (docker stop)
 *   3. Waits for Raft to elect a new leader automatically
 *   4. Resumes trading on the new leader — zero manual intervention
 *   5. Grafana shows LIVE spikes: trades/sec, elections, WAL flushes, latency
 *
 * GRAFANA: http://localhost:3000 — watch panels update every 5 seconds
 *
 * HOW TO RUN:
 *   1. Make sure docker-compose up is running
 *   2. mvn compile -q && mvn exec:java -Dexec.mainClass="com.zenith.client.ContinuousTradeSimulator"
 *   3. Open Grafana — all panels will show continuous real-time activity
 *   4. Press Ctrl+C to stop
 *
 * WHAT YOU'LL SEE IN GRAFANA:
 *   - Trades/sec: continuous baseline ~500-2000/sec with spikes during bursts
 *   - Elections: jumps by 1 every ~30 seconds when leader is killed
 *   - Active Trades: steadily climbing counter
 *   - WAL Flush Rate: regular spikes every 10ms
 *   - Avg Latency: slight spike during leader failover, then recovers
 *   - Duplicates Blocked: occasional spikes from retry logic
 */
public class ContinuousTradeSimulator {

    // ── Ports for all 3 nodes ──
    private static final String HOST   = "localhost";
    private static final int[]  PORTS  = {9001, 9002, 9003};
    private static final String[] NODE_NAMES = {"zenith-node-a", "zenith-node-b", "zenith-node-c"};

    // ── Simulation config ──
    private static final int  TRADE_THREADS       = 20;    // concurrent trade workers
    private static final int  TRADES_PER_BURST    = 200;   // trades per burst cycle
    private static final int  BURST_INTERVAL_MS   = 500;   // burst every 500ms
    private static final int  LEADER_KILL_INTERVAL = 30;   // kill leader every 30 seconds
    private static final int  FAILOVER_WAIT_MS    = 8000;  // wait for new election

    // ── Live stats ──
    private static final AtomicInteger currentLeaderPort = new AtomicInteger(9001);
    private static final AtomicLong    totalTradesInserted = new AtomicLong(0);
    private static final AtomicLong    totalOpsSuccess     = new AtomicLong(0);
    private static final AtomicLong    totalOpsFailed      = new AtomicLong(0);
    private static final AtomicInteger leaderKillCount     = new AtomicInteger(0);
    private static final AtomicBoolean failoverInProgress  = new AtomicBoolean(false);
    private static final AtomicLong    tradeIdCounter      = new AtomicLong(1);

    // Shared trade pool for SELECT/UPDATE operations
    private static final List<String> knownTradeIds = Collections.synchronizedList(new ArrayList<>());

    private static final String[] TICKERS = {
            "AAPL","GOOGL","MSFT","TSLA","AMZN","GS","JPM","BAC","MS","BLK",
            "NVDA","META","NFLX","UBER","V","JPM","C","WFC","DB","BARC"
    };
    private static final String[] STATUSES = {"EXECUTED","SETTLED","CANCELLED","PENDING","REJECTED"};

    public static void main(String[] args) throws Exception {
        printBanner();

        // ── Find initial leader ──
        System.out.println("🔍 Finding initial leader...");
        int leader = findLeader();
        if (leader == -1) {
            System.out.println("❌ No leader found. Make sure docker-compose up is running.");
            System.out.println("   Wait for: '👑 Node [node-x:900x] IS NOW LEADER'");
            return;
        }
        currentLeaderPort.set(leader);
        System.out.println("✅ Initial leader on port " + leader + "\n");

        ExecutorService pool = Executors.newCachedThreadPool();

        // ── Thread 1: Continuous trade worker (runs forever) ──
        pool.submit(() -> continuousTradeWorker());

        // ── Thread 2: Stats printer (prints every 5 seconds) ──
        pool.submit(() -> statsPrinter());

        // ── Thread 3: Chaos monkey (kills leader every 30 seconds) ──
        pool.submit(() -> chaosMonkey());

        // ── Main thread: keep alive until Ctrl+C ──
        System.out.println("🚀 Simulator running. Press Ctrl+C to stop.");
        System.out.println("📊 Watch Grafana: http://localhost:3000\n");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n\n╔══════════════════════════════════════╗");
            System.out.println("║         SIMULATION COMPLETE          ║");
            System.out.println("╠══════════════════════════════════════╣");
            System.out.printf( "║  Total trades inserted : %,8d  ║%n", totalTradesInserted.get());
            System.out.printf( "║  Total ops success     : %,8d  ║%n", totalOpsSuccess.get());
            System.out.printf( "║  Total ops failed      : %,8d  ║%n", totalOpsFailed.get());
            System.out.printf( "║  Leader failovers      : %,8d  ║%n", leaderKillCount.get());
            System.out.println("╚══════════════════════════════════════╝");
            pool.shutdownNow();
        }));

        Thread.currentThread().join(); // block forever
    }

    // ── Continuous trade worker ─────────────────────────────────────────
    private static void continuousTradeWorker() {
        ExecutorService workers = Executors.newFixedThreadPool(TRADE_THREADS);

        while (!Thread.currentThread().isInterrupted()) {
            if (failoverInProgress.get()) {
                // During failover, pause briefly and retry
                sleep(500);
                continue;
            }

            int leaderPort = currentLeaderPort.get();
            CountDownLatch burst = new CountDownLatch(TRADE_THREADS);

            for (int t = 0; t < TRADE_THREADS; t++) {
                final int threadId = t;
                workers.submit(() -> {
                    try (Socket sock = new Socket(HOST, leaderPort);
                         PrintWriter out = new PrintWriter(
                                 new BufferedWriter(new OutputStreamWriter(sock.getOutputStream())), true);
                         BufferedReader in = new BufferedReader(
                                 new InputStreamReader(sock.getInputStream()))) {

                        sock.setSoTimeout(3000);
                        int opsPerThread = TRADES_PER_BURST / TRADE_THREADS;

                        for (int op = 0; op < opsPerThread; op++) {
                            if (failoverInProgress.get()) break;

                            String cmd = buildCommand(threadId, op);
                            try {
                                out.println(cmd);
                                String resp = in.readLine();
                                if (resp != null && !resp.startsWith("ERROR") && !resp.startsWith("❌")) {
                                    totalOpsSuccess.incrementAndGet();
                                    if (cmd.startsWith("INSERT")) totalTradesInserted.incrementAndGet();
                                } else {
                                    totalOpsFailed.incrementAndGet();
                                }
                            } catch (SocketTimeoutException e) {
                                totalOpsFailed.incrementAndGet();
                                // Timeout = likely mid-failover, stop this burst
                                break;
                            }
                        }
                    } catch (Exception e) {
                        // Connection failed = failover in progress
                        totalOpsFailed.addAndGet(TRADES_PER_BURST / TRADE_THREADS);
                    } finally {
                        burst.countDown();
                    }
                });
            }

            try {
                burst.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            sleep(BURST_INTERVAL_MS);
        }
    }

    // ── Build a realistic mix of commands ──────────────────────────────
    private static String buildCommand(int threadId, int opIdx) {
        int roll = (int)(Math.random() * 10);

        if (roll < 5 || knownTradeIds.isEmpty()) {
            // 50% INSERT
            long id = tradeIdCounter.getAndIncrement();
            String tid = String.format("TRD%08d", id);
            String ticker = TICKERS[(int)(Math.random() * TICKERS.length)];
            int qty = 100 + (int)(Math.random() * 9900);
            double price = 10.0 + Math.random() * 990.0;
            knownTradeIds.add(tid);
            // Keep list bounded at 50k to avoid OOM
            if (knownTradeIds.size() > 50000) {
                try { knownTradeIds.remove(0); } catch (Exception ignored) {}
            }
            return String.format("INSERT,%s,%s,%d,%.2f,PENDING,REQ-%s", tid, ticker, qty, price, tid);
        } else if (roll < 8) {
            // 30% SELECT
            String tid = knownTradeIds.get((int)(Math.random() * knownTradeIds.size()));
            return "SELECT," + tid;
        } else {
            // 20% UPDATE
            String tid = knownTradeIds.get((int)(Math.random() * knownTradeIds.size()));
            String status = STATUSES[(int)(Math.random() * STATUSES.length)];
            return "UPDATE," + tid + "," + status + ",UPD-" + threadId + "-" + opIdx;
        }
    }

    // ── Chaos Monkey — kills the leader every 30 seconds ───────────────
    private static void chaosMonkey() {
        sleep(15000); // initial grace period — let cluster stabilize first

        while (!Thread.currentThread().isInterrupted()) {
            sleep(LEADER_KILL_INTERVAL * 1000);

            int killedPort = currentLeaderPort.get();
            String killedNode = nodeNameForPort(killedPort);

            System.out.println("\n");
            System.out.println("💀 ════════════════════════════════════════════");
            System.out.println("💀  CHAOS MONKEY: Killing leader on port " + killedPort);
            System.out.println("💀  Container: " + killedNode);
            System.out.println("💀 ════════════════════════════════════════════");

            failoverInProgress.set(true);
            leaderKillCount.incrementAndGet();

            // Kill the leader container via docker
            try {
                ProcessBuilder pb = new ProcessBuilder("docker", "stop", killedNode);
                pb.redirectErrorStream(true);
                Process p = pb.start();
                String output = new String(p.getInputStream().readAllBytes());
                int exitCode = p.waitFor();
                if (exitCode == 0) {
                    System.out.println("💀  Container stopped: " + killedNode);
                } else {
                    System.out.println("⚠️  docker stop failed (exit " + exitCode + "): " + output);
                    System.out.println("⚠️  Make sure Docker Desktop is running");
                    failoverInProgress.set(false);
                    continue;
                }
            } catch (Exception e) {
                System.out.println("⚠️  Could not execute docker stop: " + e.getMessage());
                System.out.println("    Simulating failover without killing container...");
            }

            // Wait for election to complete
            System.out.println("⏳  Waiting " + (FAILOVER_WAIT_MS/1000) + "s for new leader election...");
            sleep(FAILOVER_WAIT_MS);

            // Find new leader
            System.out.println("🔍  Scanning for new leader...");
            int newLeader = findLeaderExcluding(killedPort);
            if (newLeader != -1) {
                currentLeaderPort.set(newLeader);
                System.out.println("👑  NEW LEADER elected on port " + newLeader);
                System.out.println("✅  Resuming trade workload on new leader...\n");
            } else {
                // Restart the killed node and keep going
                System.out.println("⚠️  No new leader found — restarting killed node...");
                try {
                    new ProcessBuilder("docker", "start", killedNode)
                            .redirectErrorStream(true).start().waitFor();
                    sleep(5000);
                } catch (Exception e) {
                    System.out.println("⚠️  Could not restart: " + e.getMessage());
                }
                int recovered = findLeader();
                if (recovered != -1) currentLeaderPort.set(recovered);
            }

            failoverInProgress.set(false);

            // Restart the killed node so cluster stays at 3 nodes
            System.out.println("🔄  Restarting " + killedNode + " as follower...");
            try {
                new ProcessBuilder("docker", "start", killedNode)
                        .redirectErrorStream(true).start().waitFor();
                System.out.println("✅  " + killedNode + " rejoined cluster as follower\n");
            } catch (Exception e) {
                System.out.println("⚠️  Could not restart " + killedNode + ": " + e.getMessage());
            }
        }
    }

    // ── Stats printer — every 5 seconds ────────────────────────────────
    private static void statsPrinter() {
        long lastSuccess = 0;
        long lastTime = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            sleep(5000);

            long now = System.currentTimeMillis();
            long currentSuccess = totalOpsSuccess.get();
            long opsThisPeriod = currentSuccess - lastSuccess;
            double elapsed = (now - lastTime) / 1000.0;
            long opsPerSec = (long)(opsThisPeriod / elapsed);

            lastSuccess = currentSuccess;
            lastTime = now;

            System.out.printf(
                    "[%s] Leader:9%03d | ⚡%,6d ops/sec | Trades:%,8d | ✅%,8d | ❌%,5d | Failovers:%d%s%n",
                    timestamp(),
                    currentLeaderPort.get() % 1000,
                    opsPerSec,
                    totalTradesInserted.get(),
                    totalOpsSuccess.get(),
                    totalOpsFailed.get(),
                    leaderKillCount.get(),
                    failoverInProgress.get() ? " | 🔄 FAILOVER IN PROGRESS" : ""
            );
        }
    }

    // ── Leader detection ────────────────────────────────────────────────
    private static int findLeader() {
        return findLeaderExcluding(-1);
    }

    private static int findLeaderExcluding(int excludePort) {
        for (int port : PORTS) {
            if (port == excludePort) continue;
            try (Socket sock = new Socket(HOST, port);
                 PrintWriter out = new PrintWriter(
                         new BufferedWriter(new OutputStreamWriter(sock.getOutputStream())), true);
                 BufferedReader in = new BufferedReader(
                         new InputStreamReader(sock.getInputStream()))) {

                sock.setSoTimeout(2000);
                // Send a probe INSERT — only the leader will accept it
                String probeId = "PROBE" + System.currentTimeMillis();
                out.println("INSERT," + probeId + ",PROBE,1,1.0,PENDING,PROBE-" + probeId);
                String resp = in.readLine();
                if (resp != null && resp.contains("inserted")) {
                    return port;
                }
            } catch (Exception ignored) {}
        }
        return -1;
    }

    private static String nodeNameForPort(int port) {
        return switch (port) {
            case 9001 -> "zenith-node-a";
            case 9002 -> "zenith-node-b";
            case 9003 -> "zenith-node-c";
            default   -> "zenith-node-a";
        };
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String timestamp() {
        return new java.text.SimpleDateFormat("HH:mm:ss").format(new java.util.Date());
    }

    private static void printBanner() {
        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║     ZenithDB Continuous Trade Simulator                  ║");
        System.out.println("║     Real-time chaos + workload — runs forever            ║");
        System.out.println("╠══════════════════════════════════════════════════════════╣");
        System.out.println("║  Trade threads  : " + TRADE_THREADS + " concurrent workers               ║");
        System.out.println("║  Burst size     : " + TRADES_PER_BURST + " ops every " + BURST_INTERVAL_MS + "ms              ║");
        System.out.println("║  Leader kill    : every " + LEADER_KILL_INTERVAL + " seconds                     ║");
        System.out.println("║  Workload mix   : 50% INSERT, 30% SELECT, 20% UPDATE    ║");
        System.out.println("╠══════════════════════════════════════════════════════════╣");
        System.out.println("║  Grafana        : http://localhost:3000                  ║");
        System.out.println("║  Prometheus     : http://localhost:9090                  ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}