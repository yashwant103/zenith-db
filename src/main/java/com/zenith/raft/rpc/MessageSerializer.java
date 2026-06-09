//package com.zenith.raft.rpc;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//public class MessageSerializer {
//
//    // A single, reusable ObjectMapper instance (Thread-safe and fast)
//    private static final ObjectMapper mapper = new ObjectMapper();
//
//    // Convert a Java Object (VoteRequest/VoteResponse) into a JSON String
//    public static String serialize(RaftMessage message) {
//        try {
//            return mapper.writeValueAsString(message);
//        } catch (JsonProcessingException e) {
//            System.err.println("Failed to serialize message: " + e.getMessage());
//            return null;
//        }
//    }
//
//    // Convert a JSON String back into the correct Java Object
//    public static RaftMessage deserialize(String json) {
//        try {
//            // Notice we tell Jackson to build a base 'RaftMessage'
//            // Jackson will look at the "type" field in the JSON to figure out the exact subclass!
//            return mapper.readValue(json, RaftMessage.class);
//        } catch (JsonProcessingException e) {
//            System.err.println("Failed to deserialize JSON: " + e.getMessage());
//            return null;
//        }
//    }
//
//    // --- QUICK TEST ---
//    public static void main(String[] args) {
//        System.out.println("Testing Jackson Polymorphic Serialization...");
//
//        // 1. Create a mock VoteRequest
//        VoteRequest req = new VoteRequest("Node-A", 5, 100, 4);
//
//        // 2. Serialize to JSON
//        String json = serialize(req);
//        System.out.println("Outgoing Network String: " + json);
//
//        // 3. Deserialize back to Java Object
//        RaftMessage restoredMsg = deserialize(json);
//
//        // 4. Verify Jackson figured out the subclass automatically!
//        if (restoredMsg instanceof VoteRequest) {
//            VoteRequest restoredReq = (VoteRequest) restoredMsg;
//            System.out.println("Success! Jackson auto-detected a VoteRequest.");
//            System.out.println("Restored Sender ID: " + restoredReq.senderId);
//            System.out.println("Restored Last Log Index: " + restoredReq.lastLogIndex);
//        } else {
//            System.out.println("Failed! Jackson didn't build the right object.");
//        }
//    }
//}

package com.zenith.raft.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * MessageSerializer — converts RaftMessage objects ↔ JSON bytes.
 *
 * THIS IS THE MISSING FILE from your IntelliJ structure.
 * Your folder showed MessageSerializer but it contained Main.java code.
 *
 * WHY IS THIS NEEDED?
 * Java sockets send/receive bytes — not objects.
 * When node-1 sends a VoteRequest to node-2:
 *   node-1: VoteRequest object → serialize → bytes → socket
 *   node-2: socket → bytes → deserialize → VoteRequest object
 *
 * SINGLETON PATTERN for ObjectMapper:
 * ObjectMapper is expensive to create (loads class metadata, registers modules).
 * Creating one per message would be 1000x slower under load.
 * One static instance shared across all calls is the correct pattern.
 * ObjectMapper is thread-safe for reading — safe to share.
 *
 * LENGTH-PREFIX PROTOCOL:
 * We send [4 bytes: message length][N bytes: JSON]
 * The receiver reads 4 bytes first to know how many bytes to read next.
 * Without this, the receiver doesn't know where one message ends and
 * the next begins — essential for streaming protocols.
 */
public class MessageSerializer {

    // Single shared instance — thread-safe for concurrent serialization
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Serialize: RaftMessage → JSON string (for logging/debugging) ──
    public static String toJson(RaftMessage message) {
        try {
            return MAPPER.writeValueAsString(message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize: " + message.getClass().getSimpleName(), e);
        }
    }

    // ── Serialize: RaftMessage → byte[] (for socket transmission) ──
    public static byte[] toBytes(RaftMessage message) {
        try {
            return MAPPER.writeValueAsBytes(message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize to bytes", e);
        }
    }

    // ── Deserialize: JSON string → RaftMessage (correct subclass) ──
    // Jackson reads the "type" field, creates the matching subclass via @JsonSubTypes
    public static RaftMessage fromJson(String json) {
        try {
            return MAPPER.readValue(json, RaftMessage.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize JSON: " + json, e);
        }
    }

    // ── Deserialize: byte[] → RaftMessage ──
    public static RaftMessage fromBytes(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, RaftMessage.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize bytes", e);
        }
    }

    // ── Pretty print for debugging ──
    public static String toPrettyJson(RaftMessage message) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(message);
        } catch (Exception e) {
            return "Cannot serialize: " + e.getMessage();
        }
    }
}