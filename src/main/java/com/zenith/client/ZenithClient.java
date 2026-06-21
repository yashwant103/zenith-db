package com.zenith.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.UUID;

public class ZenithClient {
    public static void main(String[] args) {
        int targetPort = 9001; // Ensure this is pointing to your active Leader
        System.out.println("🛒 Booting Zenith Client...");

        try (Socket socket = new Socket("127.0.0.1", targetPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            System.out.println("🔗 Connected to port " + targetPort);

            // 1. Generate a Unique Request ID for this specific action
            String requestId = "REQ-" + UUID.randomUUID().toString().substring(0, 8);

            String tradeCommand = "INSERT,T105,AAPL,100,150.00,PENDING," + requestId;

            System.out.println("📤 Attempt 1: " + tradeCommand);
            out.println(tradeCommand);
            System.out.println("📥 Server: " + in.readLine());

            // ── FIX: Wait for the cluster to actually process the trade! ──
            System.out.println("\n⏳ Waiting 2 seconds to simulate network timeout...");
            Thread.sleep(2000);

            // 2. SIMULATE A NETWORK FAILURE / PANICKED RETRY
            System.out.println("⚠️ Client is panicking. Retrying exact same trade...");

            out.println(tradeCommand);
            System.out.println("📥 Server: " + in.readLine());

        } catch (Exception e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
        }
    }
}