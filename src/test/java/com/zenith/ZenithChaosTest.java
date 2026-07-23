package com.zenith;

import com.zenith.metrics.ZenithMetrics;
import com.zenith.raft.ClusterSimulator;
import com.zenith.raft.RaftNode;
import com.zenith.raft.state.RaftState;
import com.zenith.storage.MemoryEngine;
import com.zenith.wal.WriteAheadLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ZenithChaosTest {

    // FIX: track WALs so we can close them after the test
    // Old code leaked ScheduledExecutorService threads — one per WAL, never stopped
    private final List<WriteAheadLog> openWals = new ArrayList<>();

    // FIX: JUnit 5 @TempDir creates an isolated temp directory per test run
    // WAL files are written here instead of the project root
    // Directory and all files are automatically deleted after the test finishes
    @TempDir
    Path tempDir;

    @AfterEach
    void cleanup() {
        // FIX: close all WALs after each test so flusher threads don't leak
        for (WriteAheadLog wal : openWals) {
            try { wal.close(); } catch (Exception ignored) {}
        }
        // FIX: clear ClusterSimulator activeNodes so tests don't bleed into each other
        ClusterSimulator.activeNodes.clear();
    }

    @Test
    public void testLeadershipElectionAndRecovery() throws Exception {

        System.out.println("🧪 Starting Leadership Chaos Test...");

        List<String> peers = Arrays.asList("node1", "node2", "node3");

        Map<String, RaftNode> cluster = new HashMap<>();

        for (String id : peers) {

            List<String> otherPeers = new ArrayList<>(peers);
            otherPeers.remove(id);

            Thread.sleep(500);

            ZenithMetrics metrics = new ZenithMetrics(id);

            WriteAheadLog wal = makeWal(id);

            RaftNode node = new RaftNode(
                    id,
                    otherPeers,
                    new RaftState(),
                    new MemoryEngine(),
                    wal,
                    metrics
            );

            cluster.put(id, node);

            ClusterSimulator.activeNodes.put(id, node);

            node.start();
        }

        System.out.println("⏳ Waiting for stable election...");
        Thread.sleep(8000);

        RaftNode firstLeader = null;
        String leaderId = null;

        for (Map.Entry<String, RaftNode> entry : cluster.entrySet()) {

            if (entry.getValue().isLeader()) {

                firstLeader = entry.getValue();
                leaderId = entry.getKey();
                break;
            }
        }

        assertNotNull(firstLeader, "Leader should exist.");

        System.out.println("\n👑 Initial Leader : " + leaderId);

        //----------------------------------------------------
        // Simulate a REAL crash
        //----------------------------------------------------

        System.out.println("\n💀 CRASHING LEADER : " + leaderId);

        firstLeader.shutdown();

        ClusterSimulator.activeNodes.remove(leaderId);

        //----------------------------------------------------

        System.out.println("\n⏳ Waiting for re-election...\n");

        Thread.sleep(12000);

        RaftNode secondLeader = null;
        String secondLeaderId = null;

        for (Map.Entry<String, RaftNode> entry : cluster.entrySet()) {

            if (!entry.getKey().equals(leaderId)
                    && entry.getValue().isLeader()) {

                secondLeader = entry.getValue();
                secondLeaderId = entry.getKey();
                break;
            }
        }

        assertNotNull(
                secondLeader,
                "A new leader should have been elected after the old leader crashed."
        );

        System.out.println("\n👑 New Leader : " + secondLeaderId);

        System.out.println("\n✅ Chaos Test Passed!");
    }

    // Creates a WAL with a unique file path per node inside tempDir and
    // registers it for cleanup in @AfterEach — see WriteAheadLog(Path) ctor.

    private WriteAheadLog makeWal(String nodeId) throws IOException {

        Path walPath = tempDir.resolve("zenith_" + nodeId + ".log");

        WriteAheadLog wal = new WriteAheadLog(walPath);

        openWals.add(wal);

        return wal;
    }
}