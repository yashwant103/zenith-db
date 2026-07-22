package com.zenith.raft.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * MessageSerializer — converts RaftMessage objects ↔ JSON bytes.
 *
 * NOTE: this is NOT used for peer-to-peer Raft RPC traffic anymore — that
 * path now uses BinaryMessageCodec (a genuine binary encoding, no JSON).
 * This class is kept because it's still referenced by SerializerTest.java
 * and AppendEntriesTest.java (ad-hoc smoke-test mains, not JUnit), and JSON
 * round-tripping remains a reasonable debug/tooling utility on its own —
 * just not what goes over the wire between nodes today.
 *
 * SINGLETON PATTERN for ObjectMapper:
 * ObjectMapper is expensive to create (loads class metadata, registers modules).
 * Creating one per message would be 1000x slower under load.
 * One static instance shared across all calls is the correct pattern.
 * ObjectMapper is thread-safe for reading — safe to share.
 *
 * MESSAGE FRAMING:
 * toJson()/fromJson() only handle object <-> JSON conversion — they know
 * nothing about where one message ends and the next begins on the wire.
 * Framing (deciding how much of the stream is "one message") is handled
 * by the caller: every sender in this codebase writes JSON followed by a
 * newline (see RaftNode.sendToPeer), and ZenithServer accumulates bytes
 * per connection and splits on that newline before calling fromJson().
 * A newline never appears inside the JSON itself because Jackson's
 * default writeValueAsString() output is single-line.
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