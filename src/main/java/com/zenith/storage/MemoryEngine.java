//package com.zenith.storage;
//
//
//import com.zenith.wal.WriteAheadLog;
//
//import java.io.IOException;
//import java.util.concurrent.*;
//public class MemoryEngine {
//
//    private ConcurrentHashMap<String,Trade> tradeStorer= new ConcurrentHashMap<>();
//
//    public MemoryEngine(){
//        tradeStorer= new ConcurrentHashMap<>();
//    }
//
//    public void insertTrade(Trade trade){
//        tradeStorer.putIfAbsent(trade.tradeId(),trade);
//    }
//
//    public boolean updateTradeStatus(String tradeId, String newStatus, WriteAheadLog wal) throws IOException {
//
//        Trade oldtrade= tradeStorer.get(tradeId);
//
//        if(oldtrade == null){
//            System.out.println("Trade not found: "+tradeId);
//            return false;
//        }
//        Trade newTrade = new Trade(
//                oldtrade.tradeId(),
//                oldtrade.tickerSymbol(),
//                oldtrade.amount(),
//                oldtrade.price(),
//                newStatus
//        );
//        wal.appendTrade(newTrade);
//        return tradeStorer.replace(tradeId,oldtrade,newTrade);
//
//    }
//    public void restoredtrade(Trade trade){
//        tradeStorer.put(trade.tradeId(),trade);
//    }
//
//    public Trade getTrade(String tradeId){
//        return tradeStorer.get(tradeId);
//    }
//
//}


package com.zenith.storage;

import java.util.concurrent.ConcurrentHashMap;

public class MemoryEngine {

    // No need to initialize twice — remove the constructor assignment
    private final ConcurrentHashMap<String, Trade> tradeStorer = new ConcurrentHashMap<>();

    public void insertTrade(Trade trade) {
        tradeStorer.putIfAbsent(trade.tradeId(), trade);
    }

    // FIX: removed WriteAheadLog parameter — MemoryEngine is RAM only
    // Main.java handles WAL separately after this returns true
    public boolean updateTradeStatus(String tradeId, String newStatus) {
        Trade oldTrade = tradeStorer.get(tradeId);

        if (oldTrade == null) {
            System.out.println("Trade not found: " + tradeId);
            return false;
        }

        Trade newTrade = new Trade(
                oldTrade.tradeId(),
                oldTrade.tickerSymbol(),
                oldTrade.amount(),
                oldTrade.price(),
                newStatus
        );

        // CAS swap — only replaces if current value still equals oldTrade
        // Returns false if another thread already updated this trade
        return tradeStorer.replace(tradeId, oldTrade, newTrade);
    }

    // Used during WAL recovery — always overwrites, no CAS
    public void restoredtrade(Trade trade) {
        tradeStorer.put(trade.tradeId(), trade);
    }

    public Trade getTrade(String tradeId) {
        return tradeStorer.get(tradeId);
    }
}