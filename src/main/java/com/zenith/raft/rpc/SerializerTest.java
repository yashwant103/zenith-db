package com.zenith.raft.rpc;

/**
 * SerializerTest — verifies all 4 Jackson files work together.
 *
 * Run this FIRST after copying the fixed files into your project.
 * If all 4 tests pass, your Jackson setup is correct and ready for Raft.
 *
 * HOW TO RUN:
 * Right-click SerializerTest.java in IntelliJ → Run 'SerializerTest.main()'
 *
 * EXPECTED OUTPUT:
 * ✅ Test 1 PASSED — VoteRequest serialized to JSON
 * ✅ Test 2 PASSED — VoteRequest deserialized correctly, type=VOTE_REQUEST
 * ✅ Test 3 PASSED — VoteResponse serialized and deserialized
 * ✅ Test 4 PASSED — Polymorphic deserialization works
 * All tests passed! Jackson setup is correct.
 */
public class SerializerTest {

    public static void main(String[] args) {
        int passed = 0;

        // ── Test 1: Serialize VoteRequest to JSON ──
        try {
            VoteRequest req = new VoteRequest("node-1", 3, 10, 2);
            String json = MessageSerializer.toJson(req);

            // Verify all fields appear in JSON
            assert json.contains("VOTE_REQUEST") : "Missing type field";
            assert json.contains("node-1")        : "Missing senderId";
            assert json.contains("\"term\":3")    : "Missing term";
            assert json.contains("lastLogIndex")  : "Missing lastLogIndex";

            System.out.println("✅ Test 1 PASSED — VoteRequest serialized to JSON");
            System.out.println("   JSON: " + json);
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 1 FAILED: " + e.getMessage());
        }

        // ── Test 2: Deserialize JSON back to VoteRequest ──
        try {
            VoteRequest original = new VoteRequest("node-1", 3, 10, 2);
            String json = MessageSerializer.toJson(original);

            // Deserialize as base class — Jackson picks correct subclass via "type" field
            RaftMessage deserialized = MessageSerializer.fromJson(json);

            assert deserialized instanceof VoteRequest : "Wrong type returned";
            VoteRequest restored = (VoteRequest) deserialized;
            assert "node-1".equals(restored.getSenderId()) : "senderId mismatch";
            assert restored.getTerm()         == 3  : "term mismatch";
            assert restored.getLastLogIndex() == 10 : "lastLogIndex mismatch";
            assert restored.getLastLogTerm()  == 2  : "lastLogTerm mismatch";

            System.out.println("✅ Test 2 PASSED — VoteRequest deserialized correctly, type=VOTE_REQUEST");
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 2 FAILED: " + e.getMessage());
        }

        // ── Test 3: VoteResponse round-trip ──
        try {
            VoteResponse resp = new VoteResponse("node-2", 3, true);
            String json = MessageSerializer.toJson(resp);
            RaftMessage deserialized = MessageSerializer.fromJson(json);

            assert deserialized instanceof VoteResponse : "Wrong type";
            VoteResponse restored = (VoteResponse) deserialized;
            assert "node-2".equals(restored.getSenderId()) : "senderId mismatch";
            assert restored.getTerm()       == 3    : "term mismatch";
            assert restored.isVoteGranted()         : "voteGranted mismatch";

            System.out.println("✅ Test 3 PASSED — VoteResponse serialized and deserialized");
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 3 FAILED: " + e.getMessage());
        }

        // ── Test 4: Polymorphic deserialization (core Jackson feature) ──
        // Simulate receiving raw JSON from the network without knowing the type
        try {
            String rawJson = "{\"type\":\"VOTE_REQUEST\",\"senderId\":\"node-3\",\"term\":5,"
                    + "\"lastLogIndex\":20,\"lastLogTerm\":4}";

            // Deserialize into base class — Jackson reads "type":"VOTE_REQUEST" and creates VoteRequest
            RaftMessage msg = MessageSerializer.fromJson(rawJson);

            assert msg instanceof VoteRequest        : "Expected VoteRequest, got " + msg.getClass();
            assert "node-3".equals(msg.getSenderId()) : "senderId wrong";
            assert msg.getTerm() == 5                 : "term wrong";

            System.out.println("✅ Test 4 PASSED — Polymorphic deserialization works");
            System.out.println("   Raw JSON → " + msg.getClass().getSimpleName());
            passed++;
        } catch (Exception e) {
            System.out.println("❌ Test 4 FAILED: " + e.getMessage());
        }

        // ── Summary ──
        System.out.println("\n" + "─".repeat(50));
        if (passed == 4) {
            System.out.println("✅ All 4 tests passed! Jackson setup is correct.");
            System.out.println("   Ready to build AppendEntriesRequest next.");
        } else {
            System.out.println("❌ " + (4 - passed) + " test(s) failed. Check the error above.");
        }
    }
}