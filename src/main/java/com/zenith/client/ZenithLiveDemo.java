package com.zenith.client;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * ZenithLiveDemo — single, consolidated demo/test tool for ZenithDB.
 *
 * Replaces QuickTest, LoadTest, MassLoadTest, and ContinuousTradeSimulator.
 * Design goals, each one addressing a real, specific issue found during
 * this project's testing:
 *
 *   1. Auto-detects the current leader instead of hardcoding a port.
 *      Every previous tool hardcoded port 9001 (node-a), which caused
 *      repeated confusion whenever leadership moved to node-b or node-c —
 *      the tool would report "not working" when the actual issue was just
 *      talking to the wrong node.
 *   2. Runs a correctness check (insert/read/update/delete/idempotency)
 *      first, using the exact connection pattern already proven reliable
 *      (QuickTest's approach) — same socket/writer/reader setup, same
 *      per-command send/receive cycle.
 *   3. Generates a MODERATE, bounded, paced load burst afterward — enough
 *      sustained activity (roughly 20-30 seconds, ~150-200 ops/sec) for
 *      every Grafana panel to show real, non-trivial data, without being
 *      a stress test. This project's own measured real ceiling under heavy
 *      concurrency was in the low hundreds of ops/sec (see BENCHMARKS.md);
 *      this stays safely under that instead of triggering the same
 *      instability a full 100-user stress test causes.
 *   4. NO container-killing / chaos-monkey logic of any kind — this tool
 *      only ever talks to the cluster over the network, never touches
 *      Docker directly.
 *   5. Every operation counts its own success/failure individually, with
 *      automatic reconnection on a broken connection — no single hiccup
 *      can cascade into a large false failure count.
 *
 * HOW TO RUN (after `docker compose up --build` has a leader elected):
 *   mvn clean compile exec:java -Dexec.mainClass="com.zenith.client.ZenithLiveDemo"
 */
public class ZenithLiveDemo {

    private static final String HOST = "localhost";
    private static final int[]  PORTS = {9001, 9002, 9003};

    private static final int LOAD_THREADS     = 10;
    private static final int OPS_PER_THREAD   = 300;  // 3,000 total ops in the load burst
    private static final int PACE_MS          = 50;   // delay between each op per thread

    public static void main(String[] args) throws Exception {
        printBanner();

        int leaderPort = findLeader();
        if (leaderPort == -1) {
            System.out.println("❌ No leader found on any of ports 9001/9002/9003.");
            System.out.println("   Make sure 'docker compose up --build' is running");
            System.out.println("   and has finished electing a leader (check the logs).");
            return;
        }
        System.out.println("✅ Leader found on port " + leaderPort + "\n");

        runCorrectnessCheck(leaderPort);
        runLoadBurst(leaderPort);

        System.out.println("\n✅ Done. Check Grafana now: http://localhost:3000");
        System.out.println("   (all panels should show real activity from this run)");
    }

    // ── Leader detection ──
    // Tries a real probe INSERT against each known port in turn. Only the
    // leader will accept it (a follower replies with an explicit "FOLLOWER"
    // error — see ZenithServer.processCommand). Unreachable/non-leader
    // nodes are skipped silently and the next port is tried.
    private static int findLeader() {
        System.out.println("🔍 Finding current leader...");
        for (int port : PORTS) {
            try (Socket socket = new Socket(HOST, port)) {
                socket.setSoTimeout(2000);
                PrintWriter out = new PrintWriter(
                        new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String probeId = "LEADERPROBE" + System.nanoTime();
                out.println("INSERT," + probeId + ",PROBE,1,1.0,PENDING,PROBE-" + probeId);
                String resp = in.readLine();

                if (resp != null && resp.startsWith("PENDING")) {
                    return port;
                }
            } catch (Exception ignored) {
                // this node unreachable, or explicitly rejected the probe — try the next one
            }
        }
        return -1;
    }

    // ── Phase 1: correctness check ──
    private static void runCorrectnessCheck(int leaderPort) throws Exception {
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("PHASE 1 — CORRECTNESS CHECK");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        try (Socket socket = new Socket(HOST, leaderPort);
             PrintWriter out = new PrintWriter(
                     new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            socket.setSoTimeout(5000);

            send(out, in, "INSERT,DEMO001,AAPL,100,185.50,PENDING,DEMOREQ-001");
            send(out, in, "SELECT,DEMO001");
            send(out, in, "UPDATE,DEMO001,EXECUTED,DEMOUPD-001");
            send(out, in, "SELECT,DEMO001");
            System.out.println("--- Duplicate request (idempotency check) ---");
            send(out, in, "INSERT,DEMO001,AAPL,100,185.50,PENDING,DEMOREQ-001");
            send(out, in, "DELETE,DEMO001");
            send(out, in, "SELECT,DEMO001");
        }

        System.out.println();
    }

    private static void send(PrintWriter out, BufferedReader in, String cmd) throws Exception {
        System.out.println("📤 SEND: " + cmd);
        out.println(cmd);
        String response = in.readLine();
        System.out.println("📥 RECV: " + response);
    }

    // ── Phase 2: moderate, sustained, paced load burst ──
    private static void runLoadBurst(int leaderPort) throws Exception {
        int totalOpsPlanned = LOAD_THREADS * OPS_PER_THREAD;
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("PHASE 2 — LOAD BURST (" + totalOpsPlanned + " ops, ~" +
                (OPS_PER_THREAD * PACE_MS / 1000) + "s, generates real Grafana activity)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        ExecutorService pool = Executors.newFixedThreadPool(LOAD_THREADS);
        AtomicLong success = new AtomicLong(0);
        AtomicLong failure = new AtomicLong(0);
        AtomicLong totalLatencyNs = new AtomicLong(0);
        ConcurrentLinkedQueue<Long> latencySamplesNs = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(LOAD_THREADS);

        long start = System.nanoTime();

        for (int t = 0; t < LOAD_THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                Socket socket = null;
                PrintWriter out = null;
                BufferedReader in = null;
                try {
                    for (int op = 0; op < OPS_PER_THREAD; op++) {
                        String cmd;
                        int kind = op % 3;
                        if (kind == 0) {
                            String tradeId = "LOAD-" + threadId + "-" + op;
                            String reqId   = "LOADREQ-" + threadId + "-" + op;
                            cmd = "INSERT," + tradeId + ",MSFT,10," + (100 + op) + ".00,PENDING," + reqId;
                        } else if (kind == 1) {
                            cmd = "SELECT,LOAD-" + threadId + "-" + Math.max(0, op - 1);
                        } else {
                            String reqId = "LOADUPD-" + threadId + "-" + op;
                            cmd = "UPDATE,LOAD-" + threadId + "-" + Math.max(0, op - 2) + ",EXECUTED," + reqId;
                        }

                        try {
                            if (socket == null || socket.isClosed()) {
                                socket = new Socket(HOST, leaderPort);
                                socket.setSoTimeout(5000);
                                out = new PrintWriter(
                                        new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            }

                            long opStart = System.nanoTime();
                            out.println(cmd);
                            String resp = in.readLine();
                            long opEnd = System.nanoTime();
                            long latencyNs = opEnd - opStart;
                            totalLatencyNs.addAndGet(latencyNs);
                            latencySamplesNs.add(latencyNs);

                            if (resp != null && !resp.startsWith("ERROR") && !resp.startsWith("NOT_FOUND")) {
                                success.incrementAndGet();
                            } else {
                                failure.incrementAndGet();
                            }
                        } catch (Exception opEx) {
                            failure.incrementAndGet();
                            try { if (socket != null) socket.close(); } catch (Exception ignored) {}
                            socket = null; // force reconnect on the next iteration
                        }

                        Thread.sleep(PACE_MS); // paced — real sustained traffic, not a stress test
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    try { if (socket != null) socket.close(); } catch (Exception ignored) {}
                    latch.countDown();
                }
            });
        }

        latch.await(120, TimeUnit.SECONDS);
        pool.shutdown();

        long durationMs = (System.nanoTime() - start) / 1_000_000;
        long totalOps = success.get() + failure.get();
        double durationSec = durationMs / 1000.0;
        double avgLatencyMs = totalOps > 0 ? (totalLatencyNs.get() / 1_000_000.0) / totalOps : 0;

        long[] sortedLatencyNs = latencySamplesNs.stream().mapToLong(Long::longValue).sorted().toArray();
        double p50Ms = percentileMs(sortedLatencyNs, 0.50);
        double p95Ms = percentileMs(sortedLatencyNs, 0.95);
        double p99Ms = percentileMs(sortedLatencyNs, 0.99);
        double throughput = durationSec > 0 ? totalOps / durationSec : 0;
        double successThroughput = durationSec > 0 ? success.get() / durationSec : 0;

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║              BENCHMARK RESULTS — LIVE LOAD          ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.println("║  Total operations : " + String.format("%,6d", totalOps) + "                         ║");
        System.out.println("║  Duration         : " + String.format("%6.2f", durationSec) + " sec                      ║");
        System.out.println("║  Successes        : " + String.format("%,6d", success.get()) + "                         ║");
        System.out.println("║  Failures         : " + String.format("%,6d", failure.get()) + "                         ║");
        System.out.println("║  Throughput       : " + String.format("%6.2f", throughput) + " ops/sec                  ║");
        System.out.println("║  Success throughput: " + String.format("%5.2f", successThroughput) + " ops/sec                  ║");
        System.out.println("║  Avg latency      : " + String.format("%6.2f", avgLatencyMs) + " ms                       ║");
        System.out.println("║  P50 latency      : " + String.format("%6.2f", p50Ms) + " ms                       ║");
        System.out.println("║  P95 latency      : " + String.format("%6.2f", p95Ms) + " ms                       ║");
        System.out.println("║  P99 latency      : " + String.format("%6.2f", p99Ms) + " ms                       ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
    }

    private static double percentileMs(long[] sortedNs, double percentile) {
        if (sortedNs.length == 0) return 0.0;
        int index = (int) Math.ceil(percentile * sortedNs.length) - 1;
        index = Math.max(0, Math.min(index, sortedNs.length - 1));
        return sortedNs[index] / 1_000_000.0;
    }

    private static void printBanner() {
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║          ZenithDB Live Demo & Test Tool          ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
    }
}
