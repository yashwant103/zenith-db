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
    //
    // FIX: a real network partition is bidirectional — a node cut off from
    // the cluster can neither receive NOR send. This only checked whether
    // the RECIPIENT was currently active, not the SENDER. That meant a test
    // isolating a node by removing it from activeNodes correctly blocked
    // messages arriving *at* that node, but the isolated node's own
    // outbound messages to still-active peers were delivered normally —
    // an asymmetric partition, not a real one. Concretely: if the isolated
    // node had been the leader, it kept successfully delivering heartbeats
    // to the "remaining" majority, so those followers never saw a missing
    // heartbeat and never started a new election — a real automated test
    // run caught exactly this (majorityPartitionStillElectsLeader hung for
    // the full wait window because the followers correctly, but
    // misleadingly, still thought they had a healthy leader).
    public static void routeMessage(String targetNodeId, RaftMessage message) {
        if (!activeNodes.containsKey(message.getSenderId())) {
            return; // sender is currently "partitioned away" — drop silently, like a real dropped packet
        }
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
        // NOTE: peer IDs must match the keys in activeNodes (see puts below)
        // sendToPeer checks ClusterSimulator.activeNodes.containsKey(peerId) first
        // so these must be "Node-A", "Node-B", "Node-C" — NOT "node-a:9001"
        List<String> peersA = Arrays.asList("Node-B", "Node-C");
        List<String> peersB = Arrays.asList("Node-A", "Node-C");
        List<String> peersC = Arrays.asList("Node-A", "Node-B");

        // FIX: reuse the engines and WALs — don't create 6 instances
        // Old code created walA/walB/walC then passed NEW WriteAheadLog() to RaftNode
        // = 6 WAL instances all writing to zenith_db.log simultaneously → file corruption
        // = 3 leaked ScheduledExecutorService threads running forever
        //
        // REGRESSION CAUGHT DURING REVIEW: this got down to 3 WAL instances, but all
        // three still called the no-arg WriteAheadLog() constructor, which always
        // resolves to the SAME default /data/zenith_db.log file. That's still 3
        // threads flushing to one file concurrently — the identical corruption bug,
        // just with fewer culprits. WriteAheadLog(Path) exists specifically to avoid
        // this (see ZenithChaosTest.makeWal) — use it here too, one file per node.
        MemoryEngine engineA = new MemoryEngine();
        MemoryEngine engineB = new MemoryEngine();
        MemoryEngine engineC = new MemoryEngine();

        java.nio.file.Path simDir = java.nio.file.Path.of("simulator-data");
        WriteAheadLog walA = new WriteAheadLog(simDir.resolve("node-a.log"));
        WriteAheadLog walB = new WriteAheadLog(simDir.resolve("node-b.log"));
        WriteAheadLog walC = new WriteAheadLog(simDir.resolve("node-c.log"));

        RaftNode nodeA = new RaftNode("Node-A", peersA, new RaftState(), engineA, walA, new ZenithMetrics("Node-A"));
        RaftNode nodeB = new RaftNode("Node-B", peersB, new RaftState(), engineB, walB, new ZenithMetrics("Node-B"));
        RaftNode nodeC = new RaftNode("Node-C", peersC, new RaftState(), engineC, walC, new ZenithMetrics("Node-C"));

        // Plug them into the mock network
        activeNodes.put("Node-A", nodeA);
        activeNodes.put("Node-B", nodeB);
        activeNodes.put("Node-C", nodeC);

        // 🏁 START THE RACE!
        nodeA.start();
        nodeB.start();
        nodeC.start();

        // ── THE LOG REPLICATION TEST ──
        // 1. Wait 5 seconds for the cluster to elect a Leader and stabilize
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
            actualLeader.submitClientCommand("INSERT", "AAPL", "150");
        } else {
            System.out.println("❌ No leader was elected in time!");
        }

        // 3. Wait 1 second to let the network broadcast the trades to the followers
        Thread.sleep(1000);

        // FIX: close WALs cleanly so flusher threads don't leak after System.exit()
        walA.close();
        walB.close();
        walC.close();

        System.exit(0);
    }
}