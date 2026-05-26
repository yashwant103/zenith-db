package com.zenith.raft;

import com.zenith.raft.rpc.RaftMessage;
import com.zenith.raft.state.RaftState;
import com.zenith.storage.MemoryEngine;
import com.zenith.wal.WriteAheadLog;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.zenith.metrics.ZenithMetrics;
/**
 * Boots up a 3-Node Raft Cluster in memory to test Leader Election and Heartbeats.
 */
public class ClusterSimulator {

    // Our "Mock Network" router
    public static final Map<String, RaftNode> activeNodes = new ConcurrentHashMap<>();

    // This acts like our TCP socket layer for testing
    public static void routeMessage(String targetNodeId, RaftMessage message) {
        RaftNode target = activeNodes.get(targetNodeId);
        if (target != null) {
            // Simulate a tiny 10ms network delay, then drop it in their inbox
            new Thread(() -> {
                try { Thread.sleep(10); } catch (InterruptedException ignored) {}
                target.receiveMessage(message);
            }).start();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("🌐 Booting up ZenithDB Raft Cluster Simulator...\n");

        // Define our 3 nodes
        List<String> peersA = Arrays.asList("Node-B", "Node-C");
        List<String> peersB = Arrays.asList("Node-A", "Node-C");
        List<String> peersC = Arrays.asList("Node-A", "Node-B");

        // Create mock database engines and WALs for the simulator
        MemoryEngine engineA = new MemoryEngine();
        MemoryEngine engineB = new MemoryEngine();
        MemoryEngine engineC = new MemoryEngine();

        WriteAheadLog walA = new WriteAheadLog();
        WriteAheadLog walB = new WriteAheadLog();
        WriteAheadLog walC = new WriteAheadLog();

        // Pass all 5 arguments!
        RaftNode nodeA = new RaftNode("Node-A", peersA, new RaftState(), new MemoryEngine(), new WriteAheadLog(), new ZenithMetrics("Node-A"));
        RaftNode nodeB = new RaftNode("Node-B", peersB, new RaftState(), new MemoryEngine(), new WriteAheadLog(), new ZenithMetrics("Node-B"));
        RaftNode nodeC = new RaftNode("Node-C", peersC, new RaftState(), new MemoryEngine(), new WriteAheadLog(), new ZenithMetrics("Node-C"));

        // Plug them into the mock network
        activeNodes.put("Node-A", nodeA);
        activeNodes.put("Node-B", nodeB);
        activeNodes.put("Node-C", nodeC);

        // 🏁 START THE RACE!
        nodeA.start();
        nodeB.start();
        nodeC.start();

        // ── THE LOG REPLICATION TEST ──
        // 1. Wait 2 seconds for the cluster to elect a Leader and stabilize
        Thread.sleep(5000);

        System.out.println("\n🛒 [CLIENT] Finding the Leader to submit a trade...");
        RaftNode actualLeader = null;
        for (RaftNode node : activeNodes.values()) {
            if (node.isLeader()) {
                actualLeader = node;
                break;
            }
        }

        if (actualLeader != null) {
            System.out.println("🛒 [CLIENT] Found Leader! Sending trade: INSERT AAPL 150");

            // 2. The client submits the trade to the Leader
            actualLeader.submitClientCommand("INSERT", "AAPL", "150");
        } else {
            System.out.println("❌ No leader was elected in time!");
        }

        // 3. Wait 1 second to let the network broadcast the trades to the followers
        Thread.sleep(1000);

        System.exit(0);
    }
}