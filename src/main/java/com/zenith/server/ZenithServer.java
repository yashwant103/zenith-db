package com.zenith.server;

import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;
import com.zenith.wal.WriteAheadLog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class ZenithServer {
    private MemoryEngine engine;
    private WriteAheadLog log;

    public ZenithServer(MemoryEngine engine,WriteAheadLog log){
        this.engine= engine;
        this.log=log;
    }

    public void start (int port){
        try(ServerSocket serverSocket= new ServerSocket(port)){
            System.out.println("Zenith-DB Network Server Listening on port "+port+"...");
            while(true){
                Socket clientSocket= serverSocket.accept();
                System.out.println("New Connection form: "+clientSocket.getInetAddress());

                BufferedReader in = new BufferedReader(new InputStreamReader((clientSocket.getInputStream())));
                String rawCommand= in.readLine();
                if(rawCommand != null){
                    processCommand(rawCommand);
                }
                clientSocket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void processCommand(String rawCommand){
        String[] parts= rawCommand.split(",");
        if(parts[0].equals("INSERT")){
            try{
                Trade newTrade = new Trade(parts[1],parts[2],Integer.parseInt(parts[3]),Double.parseDouble(parts[4]),parts[5]);
                log.appendTrade(newTrade);
                engine.insertTrade(newTrade);
                System.out.println("SUCCESS: Inserted "+parts[1]);
            } catch (Exception e) {
                System.out.println("Error: failed to process INSERT command.");
            }
        } else if (parts[0].equals("UPDATE")) {
            String tradeId =parts[1];
            String newStatus=parts[2];

            //1. Attempt the RAM swap first
            boolean success= engine.updateTradeStatus(tradeId,newStatus);

            // 2. ONLY if the RAM swap succeeds, write it to the hard drive
            if(success){
                try{
                    Trade updateTrade = engine.getTrade(tradeId);
                    log.appendTrade(updateTrade);
                    System.out.println("SUCCESS Trade "+tradeId+" update to "+ newStatus);
                } catch (IOException e) {
                    System.out.println("CRITICAL FAULT: RAM updated but disk logging failed.");
                }
            }else {
                System.out.println("REJECTED: Could not update Trade "+tradeId+" (Cnflict or not found).");
            }
        }
    }
}
