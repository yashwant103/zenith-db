package com.zenith;

import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;
import com.zenith.wal.WriteAheadLog;

public class Main {
    public static void main(String[] args) throws Exception {
        MemoryEngine engine= new MemoryEngine();
        WriteAheadLog log = new WriteAheadLog();
        Trade trade= new Trade("T1","AAPL",100,150.0,"PENDING");

        engine.insertTrade(trade);
        log.appendTrade(trade);

        System.out.println("--- FATAL POWER LOSS ---");
        MemoryEngine recoveredEngine= new MemoryEngine();
        log.recover(recoveredEngine);
        System.out.println(recoveredEngine.getTrade("T1"));

    }
}
