package com.zenith.metrics;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ZenithMetrics — exposes Prometheus /metrics endpoint on port 8080.
 *
 * WHY THIS MATTERS FOR JPMC/GOLDMAN INTERVIEW:
 * "How do you monitor your distributed system in production?"
 * "How would you detect a leader election happening in real time?"
 * These questions expect you to know Prometheus + Grafana.
 *
 * HOW PROMETHEUS WORKS:
 * Prometheus sends GET /metrics to this endpoint every 15 seconds.
 * This class responds with all metrics in the Prometheus text format.
 * Grafana queries Prometheus and displays time-series dashboards.
 *
 * NO EXTERNAL LIBRARY NEEDED:
 * Uses Java's built-in HttpServer (java.net.httpserver) — zero dependencies.
 * In production you'd use Micrometer library for richer features,
 * but this approach is perfectly valid and shows deeper understanding.
 *
 * USAGE IN Main.java:
 *   ZenithMetrics metrics = new ZenithMetrics("Node-A");
 *   metrics.start(8080);
 *   // Then pass metrics to RaftNode and ZenithServer
 *
 * METRICS EXPOSED:
 *   zenith_trades_total          — total trades processed (counter)
 *   zenith_active_trades         — current trades in MemoryEngine (gauge)
 *   zenith_leader_elections_total — how many elections this node has started
 *   zenith_is_leader             — 1 if this node is current leader, 0 otherwise
 *   zenith_raft_term             — current Raft term number
 *   zenith_wal_flushes_total     — total WAL batch flushes to disk
 *   zenith_request_latency_ms    — average command latency in milliseconds
 */
public class ZenithMetrics {

    private final String nodeId;

    // ── Counters (only go up) ──
    public final AtomicLong tradesTotal         = new AtomicLong(0);
    public final AtomicLong electionsTotal      = new AtomicLong(0);
    public final AtomicLong walFlushesTotal     = new AtomicLong(0);
    public final AtomicLong duplicatesBlocked   = new AtomicLong(0);

    // ── Gauges (can go up and down) ──
    public final AtomicInteger activeTrades     = new AtomicInteger(0);
    public final AtomicInteger isLeader         = new AtomicInteger(0); // 1=leader, 0=follower
    public final AtomicInteger raftTerm         = new AtomicInteger(0);

    // ── Latency tracking ──
    public final AtomicLong totalLatencyMs      = new AtomicLong(0);
    public final AtomicLong latencySamples      = new AtomicLong(0);

    public ZenithMetrics(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Start the HTTP server on the given port.
     * Prometheus scrapes GET /metrics from this port.
     */
    public void start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/metrics", exchange -> {
            String body = buildMetricsResponse();
            byte[] bytes = body.getBytes();
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().close();
        });

        server.createContext("/health", exchange -> {
            String body = "OK\n";
            exchange.sendResponseHeaders(200, body.length());
            exchange.getResponseBody().write(body.getBytes());
            exchange.getResponseBody().close();
        });

        server.start();
        System.out.println("📊 Metrics server started on port " + port +
                " — Prometheus scraping at http://localhost:" + port + "/metrics");
    }

    /**
     * Builds the Prometheus text format response.
     * Format: # HELP <name> <description>
     *         # TYPE <name> <type>
     *         <name>{label="value"} <value>
     */
    private String buildMetricsResponse() {
        double avgLatency = latencySamples.get() == 0 ? 0.0
                : (double) totalLatencyMs.get() / latencySamples.get();

        return  "# HELP zenith_trades_total Total number of trades processed by this node\n" +
                "# TYPE zenith_trades_total counter\n" +
                "zenith_trades_total{node=\"" + nodeId + "\"} " + tradesTotal.get() + "\n" +

                "\n# HELP zenith_active_trades Current number of active trades in memory\n" +
                "# TYPE zenith_active_trades gauge\n" +
                "zenith_active_trades{node=\"" + nodeId + "\"} " + activeTrades.get() + "\n" +

                "\n# HELP zenith_leader_elections_total Total leader elections started by this node\n" +
                "# TYPE zenith_leader_elections_total counter\n" +
                "zenith_leader_elections_total{node=\"" + nodeId + "\"} " + electionsTotal.get() + "\n" +

                "\n# HELP zenith_is_leader Whether this node is currently the Raft leader\n" +
                "# TYPE zenith_is_leader gauge\n" +
                "zenith_is_leader{node=\"" + nodeId + "\"} " + isLeader.get() + "\n" +

                "\n# HELP zenith_raft_term Current Raft consensus term\n" +
                "# TYPE zenith_raft_term gauge\n" +
                "zenith_raft_term{node=\"" + nodeId + "\"} " + raftTerm.get() + "\n" +

                "\n# HELP zenith_wal_flushes_total Total WAL batch flushes to disk\n" +
                "# TYPE zenith_wal_flushes_total counter\n" +
                "zenith_wal_flushes_total{node=\"" + nodeId + "\"} " + walFlushesTotal.get() + "\n" +

                "\n# HELP zenith_duplicates_blocked_total Idempotency checks that blocked duplicate trades\n" +
                "# TYPE zenith_duplicates_blocked_total counter\n" +
                "zenith_duplicates_blocked_total{node=\"" + nodeId + "\"} " + duplicatesBlocked.get() + "\n" +

                "\n# HELP zenith_request_latency_ms Average command processing latency in milliseconds\n" +
                "# TYPE zenith_request_latency_ms gauge\n" +
                "zenith_request_latency_ms{node=\"" + nodeId + "\"} " +
                String.format("%.2f", avgLatency) + "\n";
    }

    // ── Helper methods ──
    public void recordTrade()         { tradesTotal.incrementAndGet(); activeTrades.incrementAndGet(); }
    public void recordElection()      { electionsTotal.incrementAndGet(); }
    public void recordWalFlush()      { walFlushesTotal.incrementAndGet(); }
    public void recordDuplicate()     { duplicatesBlocked.incrementAndGet(); }
    public void setLeader(boolean b)  { isLeader.set(b ? 1 : 0); }
    public void setRaftTerm(int term) { raftTerm.set(term); }
    public void recordLatency(long ms) {
        totalLatencyMs.addAndGet(ms);
        latencySamples.incrementAndGet();
    }
}