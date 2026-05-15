package com.zenith.raft.rpc;

import java.util.Arrays;
import java.util.List;

/**
 * AppendEntriesTest — verifies all 4 RaftMessage types serialize correctly.
 *
 * HOW TO RUN:
 * Right-click → Run 'AppendEntriesTest.main()'
 *
 * EXPECTED OUTPUT:
 * ✅ Test 1 PASSED — Heartbeat serialized, isHeartbeat=true
 * ✅ Test 2 PASSED — Log replication with 2 entries
 * ✅ Test 3 PASSED — AppendEntriesResponse success round-trip
 * ✅ Test 4 PASSED — AppendEntriesResponse failure (stale leader)
 * ✅ Test 5 PASSED — Full round trip: leader sends, follower receives correct types
 * All 5 tests passed! All 4 RPC message types are working.
 */
public class AppendEntriesTest {

    public static void main(String[] args) {
        int passed = 0;

        // ── Test 1: Heartbeat serialization ──
        try {
            AppendEntriesRequest heartbeat = AppendEntriesRequest.heartbeat(
                    "node-1", 3, 10, 2, 10
            );

            String json = MessageSerializer.toJson(heartbeat);

            assert json.contains("APPEND_ENTRIES")  : "Missing type field";
            assert json.contains("node-1")           : "Missing senderId";
            assert json.contains("\"entries\":[]")   : "entries should be empty array";

            // Deserialize and verify
            RaftMessage deserialized = MessageSerializer.fromJson(json);
            assert deserialized instanceof AppendEntriesRequest : "Wrong type";
            AppendEntriesRequest restored = (AppendEntriesRequest) deserialized;
            assert restored.isHeartbeat()              : "Should be heartbeat";
            assert restored.getPrevLogIndex() == 10    : "prevLogIndex wrong";
            assert restored.getLeaderCommit() == 10    : "leaderCommit wrong";

            System.out.println("✅ Test 1 PASSED — Heartbeat serialized, isHeartbeat=true");
            System.out.println("   JSON: " + json);
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 1 FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        // ── Test 2: Log replication with entries ──
        try {
            List<AppendEntriesRequest.LogEntryDTO> entries = Arrays.asList(
                    AppendEntriesRequest.LogEntryDTO.set(11, 3, "T5", "EXECUTED", 42L),
                    AppendEntriesRequest.LogEntryDTO.set(12, 3, "T6", "PENDING",  43L)
            );

            AppendEntriesRequest replication = new AppendEntriesRequest(
                    "node-1", 3, 10, 2, entries, 10
            );

            String json = MessageSerializer.toJson(replication);

            // Deserialize and verify entries survived
            RaftMessage deserialized = MessageSerializer.fromJson(json);
            AppendEntriesRequest restored = (AppendEntriesRequest) deserialized;

            assert !restored.isHeartbeat()                : "Should NOT be heartbeat";
            assert restored.getEntries().size() == 2      : "Should have 2 entries";
            assert "T5".equals(restored.getEntries().get(0).getKey())   : "First key wrong";
            assert restored.getEntries().get(0).getSequenceId() == 42L  : "seqId wrong";
            assert "DELETE".equals(
                    AppendEntriesRequest.LogEntryDTO.delete(13, 3, "T7", 44L).getOperation()
            ) : "DELETE operation wrong";

            System.out.println("✅ Test 2 PASSED — Log replication with 2 entries");
            System.out.println("   Entries: " + restored.getEntries());
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 2 FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        // ── Test 3: AppendEntriesResponse success ──
        try {
            AppendEntriesResponse response = AppendEntriesResponse.accepted("node-2", 3, 11);
            String json = MessageSerializer.toJson(response);

            RaftMessage deserialized = MessageSerializer.fromJson(json);
            assert deserialized instanceof AppendEntriesResponse : "Wrong type";
            AppendEntriesResponse restored = (AppendEntriesResponse) deserialized;

            assert restored.isSuccess()           : "Should be success";
            assert restored.getMatchIndex() == 11 : "matchIndex wrong";
            assert restored.getTerm()       == 3  : "term wrong";
            assert "node-2".equals(restored.getSenderId()) : "senderId wrong";

            System.out.println("✅ Test 3 PASSED — AppendEntriesResponse success round-trip");
            System.out.println("   JSON: " + json);
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 3 FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        // ── Test 4: AppendEntriesResponse failure (stale leader) ──
        try {
            // Follower has higher term than the "leader" — leader is stale
            AppendEntriesResponse response = AppendEntriesResponse.staleLeader("node-2", 5);
            String json = MessageSerializer.toJson(response);

            RaftMessage deserialized = MessageSerializer.fromJson(json);
            AppendEntriesResponse restored = (AppendEntriesResponse) deserialized;

            assert !restored.isSuccess()          : "Should be failure";
            assert restored.getTerm()       == 5  : "Should carry follower's higher term";
            assert restored.getMatchIndex() == -1 : "matchIndex should be -1 for stale leader";

            System.out.println("✅ Test 4 PASSED — AppendEntriesResponse failure (stale leader)");
            System.out.println("   JSON: " + json);
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 4 FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        // ── Test 5: Full round trip simulation ──
        // Simulates: leader builds heartbeat → sends JSON → follower receives and identifies type
        try {
            // Leader side — build and serialize
            AppendEntriesRequest heartbeat = AppendEntriesRequest.heartbeat("node-1", 3, 10, 2, 10);
            byte[] networkBytes = MessageSerializer.toBytes(heartbeat);

            // Follower side — receive bytes and identify type without knowing it upfront
            RaftMessage received = MessageSerializer.fromBytes(networkBytes);

            // Follower's switch statement — how RaftNode will handle messages
            String handlerCalled;
            if (received instanceof VoteRequest) {
                handlerCalled = "handleVoteRequest";
            } else if (received instanceof VoteResponse) {
                handlerCalled = "handleVoteResponse";
            } else if (received instanceof AppendEntriesRequest req) {
                handlerCalled = req.isHeartbeat() ? "handleHeartbeat" : "handleLogReplication";
            } else if (received instanceof AppendEntriesResponse) {
                handlerCalled = "handleAppendEntriesResponse";
            } else {
                handlerCalled = "unknown";
            }

            assert "handleHeartbeat".equals(handlerCalled) : "Wrong handler called: " + handlerCalled;

            System.out.println("✅ Test 5 PASSED — Full round trip: leader sends, follower receives correct types");
            System.out.println("   Handler called: " + handlerCalled);
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 5 FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        // ── Summary ──
        System.out.println("\n" + "─".repeat(55));
        if (passed == 5) {
            System.out.println("✅ All 5 tests passed! All 4 RPC message types are working.");
            System.out.println("   Next: Build RaftNode — leader election + heartbeat loop.");
        } else {
            System.out.println("❌ " + (5 - passed) + " test(s) failed.");
        }
    }
}