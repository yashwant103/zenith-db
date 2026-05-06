//package com.zenith.wal;
//
//import com.zenith.storage.MemoryEngine;
//import com.zenith.storage.Trade;
//
//import java.io.IOException;
//import java.io.RandomAccessFile;
//
//public class WriteAheadLog implements AutoCloseable{
//
//    RandomAccessFile file;
//
//    public WriteAheadLog() throws IOException{
//        file = new RandomAccessFile("zenith_db.log","rw");
//    }
//
//    public void appendTrade(Trade trade) throws IOException {
//        String logEntry= trade.tradeId()+","+
//                trade.tickerSymbol()+","+
//                trade.amount()+","+
//                trade.price()+","+
//                trade.status()+"\n";
//        file.writeBytes(logEntry);
//        file.getFD().sync();
//    }
//
//    public void recover(MemoryEngine engine) throws IOException{
//        file.seek(0);
//        String line;
//        while((line = file.readLine()) != null){
//            String[] parts = line.split(",");
//            int parseAmount= Integer.parseInt(parts[2]);
//            double parsePrice= Double.parseDouble(parts[3]);
//
//            Trade newtrade= new Trade(
//                parts[0],
//                parts[1],
//                parseAmount,
//                parsePrice,
//                parts[4]
//            );
//            engine.restoredtrade(newtrade);
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//
//    }
//}


package com.zenith.wal;

import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * WriteAheadLog — durability layer for ZenithDB.
 *
 * Core guarantee: every trade is written to disk BEFORE being applied
 * to MemoryEngine. If the process crashes mid-operation, WAL lets us
 * recover the exact state on restart.
 *
 * Fixed issues from v1:
 * 1. Added getFD().sync() — forces OS to flush bytes to physical disk
 * 2. Implements AutoCloseable — no resource leaks
 * 3. Clear recovery comments — intentional put() over putIfAbsent()
 */
public class WriteAheadLog implements AutoCloseable {

    private final RandomAccessFile file;

    public WriteAheadLog() throws IOException {
        file = new RandomAccessFile("zenith_db.log", "rw");
        // Move pointer to end of file — don't overwrite existing log on restart
        file.seek(file.length());
    }

    /**
     * Append trade to WAL and force flush to disk.
     * MUST be called BEFORE inserting/updating in MemoryEngine.
     */
    public void appendTrade(Trade trade) throws IOException {
        String logEntry = trade.tradeId()     + "," +
                trade.tickerSymbol() + "," +
                trade.amount()       + "," +
                trade.price()        + "," +
                trade.status()       + "\n";

        file.writeBytes(logEntry);

        // CRITICAL FIX: sync forces OS buffer → physical disk
        // Without this, power loss can still lose the last write
        // In banking: this is the difference between losing a trade or not
        file.getFD().sync();
    }

    /**
     * Replay WAL from beginning to restore MemoryEngine state.
     * Called once on startup before serving any requests.
     *
     * Note: uses put() (via restoredTrade) not putIfAbsent()
     * so later log entries correctly override earlier ones.
     * Example: PENDING → EXECUTED replays correctly.
     */
    public void recover(MemoryEngine engine) throws IOException {
        // Seek to beginning — read entire log from start
        file.seek(0);
        String line;
        int recoveredCount = 0;

        while ((line = file.readLine()) != null) {
            if (line.isBlank()) continue; // skip empty lines

            String[] parts = line.split(",");
            if (parts.length != 5) {
                System.err.println("Skipping corrupt WAL entry: " + line);
                continue; // skip corrupt lines — don't crash on bad data
            }

            try {
                Trade trade = new Trade(
                        parts[0],
                        parts[1],
                        Integer.parseInt(parts[2]),
                        Double.parseDouble(parts[3]),
                        parts[4]
                );
                engine.restoredtrade(trade); // put() — later entries win
                recoveredCount++;
            } catch (NumberFormatException e) {
                System.err.println("Skipping unparseable WAL entry: " + line);
            }
        }

        System.out.println("WAL recovery complete — replayed " + recoveredCount + " entries");

        // Move pointer back to end for new appends
        file.seek(file.length());
    }

    @Override
    public void close() throws IOException {
        file.getFD().sync(); // final flush before closing
        file.close();
    }
}