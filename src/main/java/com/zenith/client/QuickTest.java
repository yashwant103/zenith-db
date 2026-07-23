package com.zenith.client;

import java.io.*;
import java.net.Socket;

/**
 * QuickTest — sends a few manual commands to ZenithDB and prints responses.
 * Run this to verify the cluster is working before the full load test.
 *
 * HOW TO RUN:
 *   mvn exec:java -Dexec.mainClass="com.zenith.client.QuickTest"
 */
public class QuickTest {
    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════╗");
        System.out.println("║     ZenithDB Quick Sanity Test       ║");
        System.out.println("╚══════════════════════════════════════╝\n");

        String host = "localhost";
        int    port = 9001; // node-a (leader)

        try (Socket socket = new Socket(host, port);
             PrintWriter out  = new PrintWriter(new BufferedWriter(
                     new OutputStreamWriter(socket.getOutputStream())), true);
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream()))) {

            socket.setSoTimeout(5000);
            System.out.println("✅ Connected to ZenithDB at " + host + ":" + port + "\n");

            // 1. INSERT
            send(out, in, "INSERT,T001,AAPL,100,185.50,PENDING,REQ-001");

            // 2. INSERT another
            send(out, in, "INSERT,T002,GOOGL,50,175.00,PENDING,REQ-002");

            // 3. SELECT
            send(out, in, "SELECT,T001");

            // 4. UPDATE status
            send(out, in, "UPDATE,T001,EXECUTED,UPD-001");

            // 5. SELECT again — should show EXECUTED
            send(out, in, "SELECT,T001");

            // 6. SELECT by ticker
            send(out, in, "SELECT_TICKER,AAPL");

            // 7. Duplicate INSERT — idempotency test
            System.out.println("--- Testing idempotency (same REQ-001 again) ---");
            send(out, in, "INSERT,T001,AAPL,100,185.50,PENDING,REQ-001");

            // 8. DELETE
            send(out, in, "DELETE,T002");

            // 9. SELECT deleted — should be gone
            send(out, in, "SELECT,T002");

            System.out.println("\n✅ All tests complete! If you see responses above, your cluster works.");
            System.out.println("👉 Now run the full load test:");
            System.out.println("   mvn exec:java -Dexec.mainClass=\"com.zenith.client.LoadTest\"");
        }
    }

    private static void send(PrintWriter out, BufferedReader in, String cmd) throws Exception {
        System.out.println("📤 SEND: " + cmd);
        out.println(cmd);
        String response = in.readLine();
        System.out.println("📥 RECV: " + response);
        System.out.println();
    }
}