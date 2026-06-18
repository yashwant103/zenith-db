package com.zenith.raft;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RaftInboxTest {

    public static void main(String[] args) {
        System.out.println("Starting Raft Inbox Test...");

        // The Inbox (Thread-safe, unbounded queue)
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();

        // 1. The Core Raft Thread (The Consumer)
        Thread raftCoreThread = new Thread(() -> {
            try {
                System.out.println("Raft Core Thread: Waiting for messages...");
                while (true) {
                    // .take() BLOCKS the thread efficiently if the queue is empty
                    String message = messageQueue.take();
                    System.out.println("   [Raft Core] Processing -> " + message);

                    if (message.equals("SHUTDOWN")) {
                        System.out.println("Raft Core shutting down.");
                        break;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        raftCoreThread.start();

        // 2. The Network Thread (The Producer)
        Thread networkThread = new Thread(() -> {
            try {
                Thread.sleep(1000); // Wait 1 second

                System.out.println("🌐 Network: Dropping VoteRequest into Inbox");
                messageQueue.put("VoteRequest from Node-B");

                Thread.sleep(1500); // Wait 1.5 seconds

                System.out.println("🌐 Network: Dropping Heartbeat into Inbox");
                messageQueue.put("AppendEntries (Heartbeat) from Node-A");

                Thread.sleep(1000);

                messageQueue.put("SHUTDOWN");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        networkThread.start();
    }
}