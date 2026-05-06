package com.zenith.storage;


import com.zenith.wal.WriteAheadLog;

import java.io.IOException;
import java.util.concurrent.*;
public class MemoryEngine {

    private ConcurrentHashMap<String,Trade> tradeStorer= new ConcurrentHashMap<>();

    public MemoryEngine(){
        tradeStorer= new ConcurrentHashMap<>();
    }

    public void insertTrade(Trade trade){
        tradeStorer.putIfAbsent(trade.tradeId(),trade);
    }

    public boolean updateTradeStatus(String tradeId, String newStatus, WriteAheadLog wal) throws IOException {

        Trade oldtrade= tradeStorer.get(tradeId);

        if(oldtrade == null){
            return false;
        }
        Trade newTrade = new Trade(
                oldtrade.tradeId(),
                oldtrade.tickerSymbol(),
                oldtrade.amount(),
                oldtrade.price(),
                newStatus
        );
        wal.appendTrade(newTrade);
        return tradeStorer.replace(tradeId,oldtrade,newTrade    );
    }
    public void restoredtrade(Trade trade){
        tradeStorer.put(trade.tradeId(),trade);
    }

    public Trade getTrade(String tradeId){
        return tradeStorer.get(tradeId);
    }

}
