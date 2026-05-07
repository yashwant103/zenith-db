package com.zenith;

import com.zenith.server.ZenithServer;
import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;
import com.zenith.wal.WriteAheadLog;

public class Main {

    public static void main(String[] args) throws Exception {

        // Clean slate for testing
//        new java.io.File("zenith_db.log").delete();
//
//        // ── Phase 1: Insert + update before crash ──
//        System.out.println("═".repeat(50));
//        System.out.println("PHASE 1: Normal operations before crash");
//        System.out.println("═".repeat(50));
//
//        try (WriteAheadLog wal = new WriteAheadLog()) {
//            MemoryEngine engine = new MemoryEngine();
//
//            // 1. Insert trades (WAL first, then RAM)
//            Trade t1 = new Trade("T1", "AAPL", 100, 150.0, "PENDING");
//            Trade t2 = new Trade("T2", "RELIANCE", 50, 2800.0, "PENDING");
//
//            wal.appendTrade(t1);
//            engine.insertTrade(t1);
//            System.out.println("Inserted: " + t1);
//
//            wal.appendTrade(t2);
//            engine.insertTrade(t2);
//            System.out.println("Inserted: " + t2);
//
//            // 2. Update T1 status — Orchestrator Pattern (RAM first, then WAL)
//            boolean t1Updated = engine.updateTradeStatus("T1", "EXECUTED");
//            if (t1Updated) {
//                wal.appendTrade(engine.getTrade("T1")); // Log it ONLY if RAM swap succeeded
//            }
//            System.out.println("Updated T1 to EXECUTED: " + t1Updated);
//            System.out.println("T1 in memory: " + engine.getTrade("T1"));
//
//            // 3. Update T2 status — Orchestrator Pattern
//            boolean t2Updated = engine.updateTradeStatus("T2", "FAILED");
//            if (t2Updated) {
//                wal.appendTrade(engine.getTrade("T2"));
//            }
//            System.out.println("Updated T2 to FAILED: " + t2Updated);
//            System.out.println("T2 in memory: " + engine.getTrade("T2"));
//
//            System.out.println("\n--- FATAL POWER LOSS ---\n");
//
//        } // WAL is automatically flushed to the disk and closed right here
//
//        // ── Phase 2: Recovery ──
//        System.out.println("═".repeat(50));
//        System.out.println("PHASE 2: Recovery from WAL");
//        System.out.println("═".repeat(50));
//
//        MemoryEngine recoveredEngine = new MemoryEngine();
//        try (WriteAheadLog wal = new WriteAheadLog()) {
//            wal.recover(recoveredEngine);
//        }
//
//        // Verify statuses survived correctly
//        System.out.println("\n── Verification ──");
//        System.out.println("T1 status = " + recoveredEngine.getTrade("T1").status()
//                + " (expected: EXECUTED) "
//                + (recoveredEngine.getTrade("T1").status().equals("EXECUTED") ? "✅" : "❌"));
//        System.out.println("T2 status = " + recoveredEngine.getTrade("T2").status()
//                + " (expected: FAILED)   "
//                + (recoveredEngine.getTrade("T2").status().equals("FAILED") ? "✅" : "❌"));
//
//        // Cleanup the test file
//        new java.io.File("zenith_db.log").delete();


        try{
            System.out.println("Booting Zenith-DB...");
            MemoryEngine engine= new MemoryEngine();
            WriteAheadLog log = new WriteAheadLog();

            log.recover(engine);

            ZenithServer  server = new ZenithServer(engine,log);
            server.start(8000);
        } catch (Exception e) {
            System.out.println("Failed to boot Zenith-DB: "+e.getMessage());
        }
    }
}