package com.zenith.raft.state;

public class NodeWatcher {

    // EXPERIMENT:
    // Run this file once with the 'volatile' keyword.
    // Then, DELETE the 'volatile' keyword and run it again. Watch what happens!
    private static volatile boolean isLeader = false;

    public static void main(String[] args) {
        System.out.println("Starting NodeWatcher Exercise...");

        // Thread 1: The UI / Watcher Thread
        // It spins in an infinite loop waiting for the state to change.
        Thread watcherThread = new Thread(() -> {
            System.out.println(" Watcher Thread waiting for node to become LEADER...");
            while (!isLeader) {
                // Spinning... waiting for the CPU cache to see the update
            }
            System.out.println("Watcher Thread detected LEADER status! Stopping.");
        });

        // Thread 2: The Network Thread
        // It waits 2 seconds, then promotes the node to LEADER.
        Thread networkThread = new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Network Thread: Promoting node to LEADER now!");
            isLeader = true;
        });

        watcherThread.start();
        networkThread.start();
    }
}