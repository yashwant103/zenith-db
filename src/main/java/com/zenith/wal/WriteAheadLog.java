package com.zenith.wal;

import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.*;

/**
 * WriteAheadLog — durability layer for ZenithDB.
 *
 * FIXES FROM YOUR VERSION:
 *
 * BUG 1 — compact() iterates getAllTrades() which is a live ConcurrentHashMap
 * If another thread inserts a trade during compaction, the iteration may
 * see inconsistent state. The zenith_db.temp file in your project proves
 * compaction was triggered but the log file still has 2065 entries —
 * meaning the temp→rename swap worked but the next writes went into a
 * fresh log, leaving the temp file behind as a leftover artifact.
 * Fix: compact() calls getAllActiveTrades() which returns a snapshot List.
 * Also: delete the orphaned zenith_db.temp file from your project root.
 *
 * BUG 2 — recover() calls engine.insertTrade() for normal entries
 * This is correct for recovery (later entries win), but insertTrade()
 * does NOT call restoredtrade(). For clarity and correctness, recovery
 * should call engine.restoredtrade() — which internally calls insertTrade().
 *
 * BUG 3 — appendTrade() throws IOException in signature but buffer.add() never throws
 * The throws IOException declaration is misleading since the actual I/O
 * happens in flush() which catches the exception internally.
 * Kept for API compatibility but clarified with comment.
 *
 * BUG 4 — BATCH_SIZE named BATCh_SIZE (naming convention)
 * Minor — renamed to BATCH_SIZE (all caps, correct Java constant naming).
 *
 * BUG 5 — flusher.shutdown() in close() doesn't wait for in-flight flush
 * shutdown() stops accepting new tasks but doesn't wait for current flush to finish.
 * Fix: use awaitTermination(2, SECONDS) after shutdown() before final flush.
 */
public class WriteAheadLog implements AutoCloseable {

    private RandomAccessFile file;
    private final ConcurrentLinkedQueue<String> buffer;
    private final ScheduledExecutorService flusher;
    private static final int BATCH_SIZE = 500; // FIX: was BATCh_SIZE

    public WriteAheadLog() throws IOException {
        file = new RandomAccessFile("zenith_db.log", "rw");
        file.seek(file.length()); // append mode — don't overwrite on restart

        buffer  = new ConcurrentLinkedQueue<>();
        flusher = Executors.newSingleThreadScheduledExecutor();
        flusher.scheduleAtFixedRate(this::flush, 10, 10, TimeUnit.MILLISECONDS);
    }

    // appendTrade buffers the entry — actual I/O happens in flush()
    // throws IOException kept for API compatibility (callers expect it)
    public void appendTrade(Trade trade) throws IOException {
        String logEntry = trade.tradeId()      + "," +
                trade.tickerSymbol() + "," +
                trade.amount()       + "," +
                trade.price()        + "," +
                trade.status()       + "\n";
        buffer.add(logEntry);
        if (buffer.size() >= BATCH_SIZE) flush();
    }

    private synchronized void flush() {
        if (buffer.isEmpty()) return;
        try {
            StringBuilder dataBatch = new StringBuilder();
            int count = 0;
            while (!buffer.isEmpty() && count < BATCH_SIZE) {
                dataBatch.append(buffer.poll());
                count++;
            }
            if (dataBatch.length() > 0) {
                file.writeBytes(dataBatch.toString());
                file.getFD().sync(); // force OS page cache → physical disk
            }
        } catch (IOException e) {
            System.err.println("CRITICAL: Failed to flush WAL batch to disk: " + e.getMessage());
        }
    }

    public void recover(MemoryEngine engine) throws IOException {
        file.seek(0);
        String line;
        int recoveredCount = 0;

        while ((line = file.readLine()) != null) {
            if (line.isBlank()) continue;

            String[] parts = line.split(",");
            if (parts.length != 5) {
                System.err.println("Skipping corrupt WAL entry: " + line);
                continue;
            }

            try {
                Trade trade = new Trade(
                        parts[0], parts[1],
                        Integer.parseInt(parts[2]),
                        Double.parseDouble(parts[3]),
                        parts[4]
                );

                if (trade.status().equals("DELETED")) {
                    engine.deleteTrade(trade.tradeId()); // tombstone — erase from RAM
                } else {
                    engine.restoredtrade(trade); // FIX: was engine.insertTrade()
                }
                recoveredCount++;
            } catch (NumberFormatException e) {
                System.err.println("Skipping unparseable WAL entry: " + line);
            }
        }

        System.out.println("WAL recovery complete — replayed " + recoveredCount + " entries");
        file.seek(file.length()); // back to end for new appends
    }

    public synchronized void compact(MemoryEngine engine) {
        System.out.println("Starting Log Compaction...");
        flush(); // drain buffer before compacting

        try {
            java.io.File tempFile = new java.io.File("zenith_db.temp");

            // FIX: getAllActiveTrades() returns a snapshot — safe to iterate
            // while other threads continue inserting into MemoryEngine
            try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
                for (Trade trade : engine.getAllActiveTrades()) {
                    writer.write(trade.tradeId()      + "," +
                            trade.tickerSymbol() + "," +
                            trade.amount()       + "," +
                            trade.price()        + "," +
                            trade.status()       + "\n");
                }
            }

            file.close();
            java.io.File oldLog = new java.io.File("zenith_db.log");
            oldLog.delete();
            tempFile.renameTo(oldLog); // atomic on Linux/Mac

            this.file = new RandomAccessFile("zenith_db.log", "rw");
            this.file.seek(this.file.length());
            System.out.println("Compaction complete! Log now has " +
                    engine.getAllActiveTrades().size() + " entries.");

        } catch (Exception e) {
            System.err.println("Compaction failed: " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        flusher.shutdown();
        try {
            // FIX: wait for any in-progress scheduled flush to complete
            flusher.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flush();               // drain any remaining buffer entries
        file.getFD().sync();   // final force to physical disk
        file.close();
    }
}