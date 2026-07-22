package com.zenith;

import com.zenith.metrics.ZenithMetrics;
import com.zenith.raft.RaftNode;
import com.zenith.raft.state.RaftState;
import com.zenith.server.ZenithServer;
import com.zenith.storage.MemoryEngine;
import com.zenith.wal.WriteAheadLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        String nodeId = "127.0.0.1:9001";
        int port = 9001;
        List<String> peers = new ArrayList<>(List.of("127.0.0.1:9002", "127.0.0.1:9003"));

        if (args.length >= 2) {
            nodeId = args[0] + ":" + args[1];
            port = Integer.parseInt(args[1]);
            peers.clear();
            for (int i = 2; i < args.length; i++) {
                peers.add(args[i]);
            }
        }

        // FIX: Dynamic metrics port (e.g., NIO 9001 -> Metrics 8001) to prevent "Address already in use"
        int metricsPort = port - 1000;

        System.out.println("═".repeat(50));
        System.out.println("🌐 Booting ZenithDB on " + nodeId);
        System.out.println("🔗 Peers: " + peers);
        System.out.println("═".repeat(50));

        ZenithMetrics metrics = new ZenithMetrics(nodeId);
        try {
            metrics.start(metricsPort);
        } catch (IOException e) {
            System.err.println("Failed to start metrics server: " + e.getMessage());
        }
        metrics.setLeader(false);

        try {
            MemoryEngine engine = new MemoryEngine();
            WriteAheadLog log = new WriteAheadLog();
            log.recover(engine);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutdown signal received — flushing WAL...");
                try { log.close(); } catch (Exception e) { System.err.println("WAL close error: " + e.getMessage()); }
                System.out.println("✅ WAL flushed. Goodbye.");
            }));

            RaftState raftState = new RaftState();
            RaftNode raftNode = new RaftNode(nodeId, peers, raftState, engine, log, metrics);
            ZenithServer server = new ZenithServer(engine, log, raftNode, metrics);

            raftNode.start();
            server.start(port);
        } catch (Exception e) {
            System.out.println("❌ Failed to boot ZenithDB: " + e.getMessage());
            e.printStackTrace();
        }
    }
}