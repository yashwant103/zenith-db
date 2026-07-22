package com.zenith.wal;

import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;

/**
 * WriteAheadLog — durability layer for ZenithDB.
 *
 * Improvements:
 *
 * ✔ Configurable WAL path (for tests)
 * ✔ Default constructor still uses production /data path
 * ✔ Automatically creates parent directories
 * ✔ No behavior changes to batching/recovery/compaction
 *
 * REGRESSION CAUGHT DURING REVIEW:
 * DEFAULT_WAL_PATH had drifted back to a *relative* Path.of("data", ...),
 * which silently reintroduced the exact bug an earlier fix pass already
 * claimed to solve ("WAL file path was relative, breaking Docker volume
 * mounting"). A relative path only resolves under /data/zenith_db.log if
 * the JVM's working directory happens to equal the Dockerfile's WORKDIR
 * (/data) at the moment the file is opened — true today, but fragile and
 * silently wrong for local runs or any future entrypoint change. Restored
 * to an absolute path so WAL location is deterministic regardless of CWD.
 */
public class WriteAheadLog implements AutoCloseable {

    // Production defaults — absolute path so WAL location never depends on
    // the JVM's current working directory (see note above).
    private static final Path DEFAULT_WAL_PATH  = Path.of("/data", "zenith_db.log");
    private static final Path DEFAULT_TEMP_PATH = Path.of("/data", "zenith_db.temp");

    private final Path walPath;

    public Path getWalPath() { return walPath; }
    private final Path tempPath;

    private RandomAccessFile file;

    private final ConcurrentLinkedQueue<String> buffer;
    private final ScheduledExecutorService flusher;

    // Optional — wired up by RaftNode so WAL flushes show up in Prometheus.
    // Previously zenith_wal_flushes_total was permanently stuck at 0 because
    // nothing ever called metrics.recordWalFlush().
    private volatile com.zenith.metrics.ZenithMetrics metrics;

    private static final int BATCH_SIZE = 500;

    public void setMetrics(com.zenith.metrics.ZenithMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Production constructor.
     */
    public WriteAheadLog() throws IOException {
        this(DEFAULT_WAL_PATH);
    }

    /**
     * Test constructor.
     *
     * Example:
     *
     * temp/
     *   node1.log
     *   node2.log
     *   node3.log
     */
    public WriteAheadLog(Path walPath) throws IOException {

        this.walPath = walPath;

        String fileName = walPath.getFileName().toString();

        if (fileName.endsWith(".log")) {
            fileName = fileName.substring(0, fileName.length() - 4);
        }

        this.tempPath =
                walPath.getParent().resolve(fileName + ".temp");

        Files.createDirectories(walPath.getParent());

        file = new RandomAccessFile(walPath.toFile(), "rw");
        file.seek(file.length());

        buffer = new ConcurrentLinkedQueue<>();

        flusher = Executors.newSingleThreadScheduledExecutor();

        flusher.scheduleAtFixedRate(
                this::flush,
                10,
                10,
                TimeUnit.MILLISECONDS
        );
    }

    /** Kept for backward compatibility / call sites that don't have a
     *  requestId handy — no idempotency history is recorded in that case. */
    public void appendTrade(Trade trade) throws IOException {
        appendTrade(trade, null);
    }

    /**
     * FIX (idempotency across restart): previously this only recorded trade
     * data (ticker/amount/price/status), never which client requestId
     * produced it. That meant MemoryEngine's processedRequests set — which
     * is what makes duplicate INSERT/UPDATE/DELETE submissions safe to
     * retry — came back completely empty after every restart, even though
     * the underlying trades themselves were correctly restored. A client
     * retrying a request right after a node restart could get silently
     * reprocessed instead of correctly rejected as a duplicate.
     * requestId is now a 6th CSV field (sentinel "NONE" when absent, e.g.
     * calls that still use the old appendTrade(Trade) overload), and
     * recover() replays it into engine.markProcessed().
     */
    public void appendTrade(Trade trade, String requestId) throws IOException {

        String logEntry =
                trade.tradeId() + "," +
                        trade.tickerSymbol() + "," +
                        trade.amount() + "," +
                        trade.price() + "," +
                        trade.status() + "," +
                        (requestId == null ? "NONE" : requestId) + "\n";

        buffer.add(logEntry);

        if (buffer.size() >= BATCH_SIZE) {
            flush();
        }
    }

    // Exposed so callers can force a synchronous fsync at a specific ordering
    // point (see RaftNode.applyCommittedEntries) instead of waiting up to the
    // normal 10ms scheduled interval.
    public void flushNow() {
        flush();
    }

    private synchronized void flush() {

        if (buffer.isEmpty()) {
            return;
        }

        try {

            StringBuilder batch = new StringBuilder();

            int count = 0;

            while (!buffer.isEmpty() && count < BATCH_SIZE) {

                batch.append(buffer.poll());

                count++;
            }

            if (!batch.isEmpty()) {

                file.writeBytes(batch.toString());

                file.getFD().sync();

                if (metrics != null) {
                    metrics.recordWalFlush();
                }
            }

        } catch (IOException e) {

            System.err.println(
                    "CRITICAL: Failed to flush WAL batch: "
                            + e.getMessage()
            );
        }
    }

    public void recover(MemoryEngine engine) throws IOException {

        file.seek(0);

        String line;

        int recovered = 0;
        int idempotencyRestored = 0;

        while ((line = file.readLine()) != null) {

            if (line.isBlank()) {
                continue;
            }

            String[] parts = line.split(",");

            // Accept both the old 5-field format (no requestId — pre-dates
            // this fix, or written via the compatibility overload) and the
            // new 6-field format.
            if (parts.length != 5 && parts.length != 6) {

                System.err.println(
                        "Skipping corrupt WAL entry: " + line
                );

                continue;
            }

            try {

                Trade trade =
                        new Trade(
                                parts[0],
                                parts[1],
                                Integer.parseInt(parts[2]),
                                Double.parseDouble(parts[3]),
                                parts[4]
                        );

                if ("DELETED".equals(trade.status())) {

                    engine.deleteTrade(trade.tradeId());

                } else {

                    engine.restoredtrade(trade);
                }

                if (parts.length == 6 && !"NONE".equals(parts[5])) {
                    engine.markProcessed(parts[5]);
                    idempotencyRestored++;
                }

                recovered++;

            } catch (NumberFormatException e) {

                System.err.println(
                        "Skipping invalid WAL entry: " + line
                );
            }
        }

        System.out.println(
                "WAL recovery complete — replayed "
                        + recovered
                        + " entries ("
                        + idempotencyRestored
                        + " with idempotency history restored)"
        );

        file.seek(file.length());
    }

    public synchronized void compact(MemoryEngine engine) {

        System.out.println("Starting Log Compaction...");

        flush();

        try {

            File tempFile = tempPath.toFile();

            try (FileWriter writer = new FileWriter(tempFile)) {

                for (Trade trade : engine.getAllActiveTrades()) {

                    writer.write(
                            trade.tradeId() + "," +
                                    trade.tickerSymbol() + "," +
                                    trade.amount() + "," +
                                    trade.price() + "," +
                                    trade.status() + "\n"
                    );
                }
            }

            file.close();

            File oldLog = walPath.toFile();

            oldLog.delete();

            tempFile.renameTo(oldLog);

            file = new RandomAccessFile(oldLog, "rw");

            file.seek(file.length());

            System.out.println(
                    "Compaction complete! Log now has "
                            + engine.getAllActiveTrades().size()
                            + " entries."
            );

        } catch (Exception e) {

            System.err.println(
                    "Compaction failed: "
                            + e.getMessage()
            );
        }
    }

    @Override
    public void close() throws IOException {

        flusher.shutdown();

        try {

            flusher.awaitTermination(
                    2,
                    TimeUnit.SECONDS
            );

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
        }

        flush();

        file.getFD().sync();

        file.close();
    }
}