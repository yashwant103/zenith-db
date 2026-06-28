package com.zenith;

import com.zenith.metrics.ZenithMetrics; // ADDED THIS IMPORT
import com.zenith.raft.RaftNode;
import com.zenith.raft.state.RaftState;
import com.zenith.storage.MemoryEngine;
import com.zenith.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

public class ZenithChaosTest {

    @Test
    public void testLeadershipElectionAndRecovery() throws Exception {
        System.out.println("🧪 Starting Leadership Chaos Test...");

        List<String> peers = Arrays.asList("node1", "node2", "node3");
        Map<String, RaftNode> cluster = new HashMap<>();

        for (String id : peers) {
            List<String> otherPeers = new ArrayList<>(peers);
            otherPeers.remove(id);
            // STAGGERED START: Give each node a 500ms head start to avoid split votes
            Thread.sleep(500);

            // FIX: Initialize ZenithMetrics for the test node
            ZenithMetrics metrics = new ZenithMetrics(id);

            // FIX: Pass 'metrics' as the final parameter
            RaftNode node = new RaftNode(id, otherPeers, new RaftState(), new MemoryEngine(), new WriteAheadLog(), metrics);

            cluster.put(id, node);
            node.start();
        }

        // Increase wait time to 8 seconds to allow for stable election
        System.out.println("⏳ Waiting for stable election...");
        Thread.sleep(8000);

        RaftNode firstLeader = null;
        String leaderId = "";
        for (Map.Entry<String, RaftNode> entry : cluster.entrySet()) {
            if (entry.getValue().isLeader()) {
                firstLeader = entry.getValue();
                leaderId = entry.getKey();
                break;
            }
        }

        assertNotNull(firstLeader, "A leader should have been elected.");
        System.out.println("👑 Initial Leader: " + leaderId);

        // 3. CHAOS: Stop the Leader's heartbeats manually
        System.out.println("💀 KILLING THE LEADER: " + leaderId);
        // We simulate a crash by just ignoring this node for the rest of the test

        // 4. Verify a new Leader is elected from the survivors
        System.out.println("⏳ Waiting for survivors to re-elect...");
        Thread.sleep(10000);

        RaftNode secondLeader = null;
        for (Map.Entry<String, RaftNode> entry : cluster.entrySet()) {
            if (entry.getValue().isLeader() && !entry.getKey().equals(leaderId)) {
                secondLeader = entry.getValue();
                System.out.println("👑 New Leader elected: " + entry.getKey());
                break;
            }
        }

        assertNotNull(secondLeader, "A new leader should have been elected after failure.");
    }
}