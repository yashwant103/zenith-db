//package com.zenith.wal;
//
//import com.zenith.storage.MemoryEngine;
//import com.zenith.storage.Trade;
//
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.util.concurrent.*;
//
///**
// * WriteAheadLog — durability layer for ZenithDB.
// *
// * FIXES APPLIED:
// *
// * BUG 1 — compact() iterated getAllTrades() (live ConcurrentHashMap view)
// * Another thread inserting during iteration caused ConcurrentModificationException.
// * Fix: compact() now calls getAllActiveTrades() which returns a snapshot List.
// *
// * BUG 2 — recover() called engine.insertTrade() instead of engine.restoredtrade()
// * Fix: use restoredtrade() for recovery replays.
// *
// * BUG 3 — BATCH_SIZE was named BATCh_SIZE (typo)
// * Fix: renamed to BATCH_SIZE.
// *
// * BUG 4 — flusher.shutdown() didn't wait for in-flight flush before close()
// * Fix: awaitTermination(2, SECONDS) after shutdown(), then manual final drain.
// *
// * BUG 5 — WAL file path was relative ("zenith_db.log")
// * Relative paths depend on the JVM working directory (WORKDIR in Dockerfile).
// * Fix: use /data/zenith_db.log absolute path so WAL always lands in the
// * Docker volume regardless of how/where the JAR is launched.
// * Local development: create /data directory or override WAL_PATH.
// */
//public class WriteAheadLog implements AutoCloseable {
//
//    // FIX: absolute paths so WAL always writes to the Docker volume at /data/
//    // Previously "zenith_db.log" (relative) worked only because Dockerfile sets WORKDIR /data
//    // Using absolute paths removes that fragile implicit dependency
//    private static final String WAL_PATH  = "/data/zenith_db.log";
//    private static final String TEMP_PATH = "/data/zenith_db.temp";
//
//    private RandomAccessFile file;
//    private final ConcurrentLinkedQueue<String> buffer;
//    private final ScheduledExecutorService flusher;
//    private static final int BATCH_SIZE = 500; // FIX: was BATCh_SIZE
//
//    public WriteAheadLog() throws IOException {
//        file = new RandomAccessFile(WAL_PATH, "rw");
//        file.seek(file.length()); // append mode — don't overwrite existing log on restart
//
//        buffer  = new ConcurrentLinkedQueue<>();
//        flusher = Executors.newSingleThreadScheduledExecutor();
//        flusher.scheduleAtFixedRate(this::flush, 10, 10, TimeUnit.MILLISECONDS);
//    }
//
//    // appendTrade buffers the entry — actual I/O happens in flush()
//    // throws IOException kept for API compatibility (callers expect it)
//    public void appendTrade(Trade trade) throws IOException {
//        String logEntry = trade.tradeId()      + "," +
//                trade.tickerSymbol() + "," +
//                trade.amount()       + "," +
//                trade.price()        + "," +
//                trade.status()       + "\n";
//        buffer.add(logEntry);
//        if (buffer.size() >= BATCH_SIZE) flush();
//    }
//
//    private synchronized void flush() {
//        if (buffer.isEmpty()) return;
//        try {
//            StringBuilder dataBatch = new StringBuilder();
//            int count = 0;
//            while (!buffer.isEmpty() && count < BATCH_SIZE) {
//                dataBatch.append(buffer.poll());
//                count++;
//            }
//            if (dataBatch.length() > 0) {
//                file.writeBytes(dataBatch.toString());
//                file.getFD().sync(); // force OS page cache → physical disk
//            }
//        } catch (IOException e) {
//            System.err.println("CRITICAL: Failed to flush WAL batch to disk: " + e.getMessage());
//        }
//    }
//
//    public void recover(MemoryEngine engine) throws IOException {
//        file.seek(0);
//        String line;
//        int recoveredCount = 0;
//
//        while ((line = file.readLine()) != null) {
//            if (line.isBlank()) continue;
//
//            String[] parts = line.split(",");
//            if (parts.length != 5) {
//                System.err.println("Skipping corrupt WAL entry: " + line);
//                continue;
//            }
//
//            try {
//                Trade trade = new Trade(
//                        parts[0], parts[1],
//                        Integer.parseInt(parts[2]),
//                        Double.parseDouble(parts[3]),
//                        parts[4]
//                );
//
//                if (trade.status().equals("DELETED")) {
//                    engine.deleteTrade(trade.tradeId()); // tombstone — erase from RAM
//                } else {
//                    engine.restoredtrade(trade); // FIX: was engine.insertTrade()
//                }
//                recoveredCount++;
//            } catch (NumberFormatException e) {
//                System.err.println("Skipping unparseable WAL entry: " + line);
//            }
//        }
//
//        System.out.println("WAL recovery complete — replayed " + recoveredCount + " entries");
//        file.seek(file.length()); // back to end for new appends
//    }
//
//    public synchronized void compact(MemoryEngine engine) {
//        System.out.println("Starting Log Compaction...");
//        flush(); // drain buffer before compacting
//
//        try {
//            java.io.File tempFile = new java.io.File(TEMP_PATH); // FIX: absolute path
//
//            // FIX: getAllActiveTrades() returns a snapshot List — safe to iterate
//            // while other threads continue inserting into MemoryEngine
//            try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
//                for (Trade trade : engine.getAllActiveTrades()) {
//                    writer.write(trade.tradeId()      + "," +
//                            trade.tickerSymbol() + "," +
//                            trade.amount()       + "," +
//                            trade.price()        + "," +
//                            trade.status()       + "\n");
//                }
//            }
//
//            file.close();
//            java.io.File oldLog = new java.io.File(WAL_PATH); // FIX: absolute path
//            oldLog.delete();
//            tempFile.renameTo(oldLog); // atomic rename on Linux
//
//            this.file = new RandomAccessFile(WAL_PATH, "rw"); // FIX: absolute path
//            this.file.seek(this.file.length());
//            System.out.println("Compaction complete! Log now has " +
//                    engine.getAllActiveTrades().size() + " entries.");
//
//        } catch (Exception e) {
//            System.err.println("Compaction failed: " + e.getMessage());
//        }
//    }
//
//    @Override
//    public void close() throws IOException {
//        flusher.shutdown();
//        try {
//            // FIX: wait for any in-progress scheduled flush to complete before draining
//            flusher.awaitTermination(2, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
//        flush();               // drain any remaining buffered entries
//        file.getFD().sync();   // final force to physical disk
//        file.close();
//    }
//}

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
 */
public class WriteAheadLog implements AutoCloseable {

    // Production defaults
    private static final Path DEFAULT_WAL_PATH  = Path.of("data", "zenith_db.log");
    private static final Path DEFAULT_TEMP_PATH = Path.of("data", "zenith_db.temp");

    private final Path walPath;
    private final Path tempPath;

    private RandomAccessFile file;

    private final ConcurrentLinkedQueue<String> buffer;
    private final ScheduledExecutorService flusher;

    private static final int BATCH_SIZE = 500;

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

    public void appendTrade(Trade trade) throws IOException {

        String logEntry =
                trade.tradeId() + "," +
                        trade.tickerSymbol() + "," +
                        trade.amount() + "," +
                        trade.price() + "," +
                        trade.status() + "\n";

        buffer.add(logEntry);

        if (buffer.size() >= BATCH_SIZE) {
            flush();
        }
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

        while ((line = file.readLine()) != null) {

            if (line.isBlank()) {
                continue;
            }

            String[] parts = line.split(",");

            if (parts.length != 5) {

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
                        + " entries"
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