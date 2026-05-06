package com.zenith.wal;

import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;

import java.io.IOException;
import java.io.RandomAccessFile;

public class WriteAheadLog {

    RandomAccessFile file;

    public WriteAheadLog() throws IOException{
        file = new RandomAccessFile("zenith_db.log","rw");
    }

    public void appendTrade(Trade trade) throws IOException {
        String logEntry= trade.tradeId()+","+
                trade.tickerSymbol()+","+
                trade.amount()+","+
                trade.price()+","+
                trade.status()+"\n";
        file.writeBytes(logEntry);
    }

    public void recover(MemoryEngine engine) throws IOException{
        file.seek(0);
        String line;
        while((line = file.readLine()) != null){
            String[] parts = line.split(",");
            int parseAmount= Integer.parseInt(parts[2]);
            double parsePrice= Double.parseDouble(parts[3]);

            Trade newtrade= new Trade(
                parts[0],
                parts[1],
                parseAmount,
                parsePrice,
                parts[4]
            );
            engine.restoredtrade(newtrade);
        }
    }

}
