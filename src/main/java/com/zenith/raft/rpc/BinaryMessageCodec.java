package com.zenith.raft.rpc;

import com.zenith.raft.rpc.AppendEntriesRequest.LogEntryDTO;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * BinaryMessageCodec — the actual binary wire protocol for Raft peer-to-peer
 * RPCs (VoteRequest/Response, AppendEntries Request/Response).
 *
 * This replaces MessageSerializer's JSON encoding for the peer-to-peer path.
 * Client commands (INSERT/SELECT/etc., typed by a human or ZenithClient)
 * deliberately stay as plain newline-delimited text — that's a separate,
 * simpler protocol serving a different purpose (human-typeable demo/debug
 * access), not what this binary format is for. Multiplexing between the two
 * on the same TCP port is handled in ZenithServer via a magic marker byte
 * (see FRAME_MAGIC) that can never collide with a legitimate text command,
 * since every text command starts with an uppercase ASCII letter.
 *
 * WIRE FORMAT (one frame = one message):
 *   byte    FRAME_MAGIC        (0x02 — distinguishes this from text commands)
 *   int32   payloadLength      (big-endian, via DataOutputStream — the
 *                                length-prefix framing a comment in the old
 *                                MessageSerializer claimed existed but never
 *                                did; this time it's real)
 *   byte[]  payload:
 *     byte    messageType      (see TYPE_* constants)
 *     UTF     senderId         (2-byte length prefix + UTF-8 bytes, via
 *                                DataOutputStream.writeUTF)
 *     int32   term
 *     ...type-specific fields (see encode/decode below)
 *
 * DESIGN CHOICE, stated plainly: this is a hand-rolled binary format using
 * Java's built-in DataOutputStream/DataInputStream primitives (writeInt,
 * writeUTF, writeLong, writeBoolean) rather than a general-purpose binary
 * serialization library (e.g. Protocol Buffers) or a bit-packed custom
 * format. That's a deliberate scope choice: it gets you a genuine binary
 * encoding — no JSON parsing, no text overhead, fixed-width integer fields
 * — without pulling in a schema compiler or hand-rolling variable-length
 * integer encoding. A protobuf/flatbuffers version would be the natural
 * next step if you wanted schema evolution guarantees (e.g. adding a field
 * without breaking old clients) — this format has no such guarantee today;
 * every node in the cluster must run the same version.
 */
public class BinaryMessageCodec {

    public static final byte FRAME_MAGIC = 0x02;

    private static final byte TYPE_VOTE_REQUEST           = 1;
    private static final byte TYPE_VOTE_RESPONSE          = 2;
    private static final byte TYPE_APPEND_ENTRIES_REQUEST = 3;
    private static final byte TYPE_APPEND_ENTRIES_RESPONSE = 4;

    public static byte[] encode(RaftMessage message) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            if (message instanceof VoteRequest m) {
                out.writeByte(TYPE_VOTE_REQUEST);
                writeHeader(out, m);
                out.writeInt(m.getLastLogIndex());
                out.writeInt(m.getLastLogTerm());

            } else if (message instanceof VoteResponse m) {
                out.writeByte(TYPE_VOTE_RESPONSE);
                writeHeader(out, m);
                out.writeBoolean(m.isVoteGranted());

            } else if (message instanceof AppendEntriesRequest m) {
                out.writeByte(TYPE_APPEND_ENTRIES_REQUEST);
                writeHeader(out, m);
                out.writeInt(m.getPrevLogIndex());
                out.writeInt(m.getPrevLogTerm());
                out.writeInt(m.getLeaderCommit());
                List<LogEntryDTO> entries = m.getEntries();
                out.writeInt(entries.size());
                for (LogEntryDTO e : entries) {
                    out.writeInt(e.getIndex());
                    out.writeInt(e.getTerm());
                    writeNullableUTF(out, e.getKey());
                    writeNullableUTF(out, e.getValue());
                    writeNullableUTF(out, e.getOperation());
                    out.writeLong(e.getSequenceId());
                }

            } else if (message instanceof AppendEntriesResponse m) {
                out.writeByte(TYPE_APPEND_ENTRIES_RESPONSE);
                writeHeader(out, m);
                out.writeBoolean(m.isSuccess());
                out.writeInt(m.getMatchIndex());

            } else {
                throw new IllegalArgumentException("Unknown RaftMessage subtype: " + message.getClass());
            }

            out.flush();
            return baos.toByteArray();

        } catch (IOException e) {
            // ByteArrayOutputStream never actually throws IOException in
            // practice, but DataOutputStream's methods declare it.
            throw new RuntimeException("Failed to encode " + message.getClass().getSimpleName(), e);
        }
    }

    public static RaftMessage decode(byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            byte type = in.readByte();
            String senderId = in.readUTF();
            int term = in.readInt();

            return switch (type) {
                case TYPE_VOTE_REQUEST -> {
                    int lastLogIndex = in.readInt();
                    int lastLogTerm = in.readInt();
                    yield new VoteRequest(senderId, term, lastLogIndex, lastLogTerm);
                }
                case TYPE_VOTE_RESPONSE -> {
                    boolean granted = in.readBoolean();
                    yield new VoteResponse(senderId, term, granted);
                }
                case TYPE_APPEND_ENTRIES_REQUEST -> {
                    int prevLogIndex = in.readInt();
                    int prevLogTerm = in.readInt();
                    int leaderCommit = in.readInt();
                    int count = in.readInt();
                    List<LogEntryDTO> entries = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        int idx = in.readInt();
                        int entryTerm = in.readInt();
                        String key = readNullableUTF(in);
                        String value = readNullableUTF(in);
                        String operation = readNullableUTF(in);
                        long seq = in.readLong();
                        entries.add(new LogEntryDTO(idx, entryTerm, key, value, operation, seq));
                    }
                    yield new AppendEntriesRequest(senderId, term, prevLogIndex, prevLogTerm, entries, leaderCommit);
                }
                case TYPE_APPEND_ENTRIES_RESPONSE -> {
                    boolean success = in.readBoolean();
                    int matchIndex = in.readInt();
                    yield new AppendEntriesResponse(senderId, term, success, matchIndex);
                }
                default -> throw new IllegalArgumentException("Unknown binary message type tag: " + type);
            };

        } catch (IOException e) {
            throw new RuntimeException("Failed to decode binary Raft message (" + payload.length + " bytes)", e);
        }
    }

    private static void writeHeader(DataOutputStream out, RaftMessage m) throws IOException {
        out.writeUTF(m.getSenderId());
        out.writeInt(m.getTerm());
    }

    // writeUTF() throws NullPointerException on a null string (LogEntryDTO.value
    // is legitimately null for DELETE entries — see LogEntryDTO.delete()), so
    // every nullable field needs an explicit has-value flag ahead of it.
    private static void writeNullableUTF(DataOutputStream out, String s) throws IOException {
        out.writeBoolean(s != null);
        if (s != null) out.writeUTF(s);
    }

    private static String readNullableUTF(DataInputStream in) throws IOException {
        boolean present = in.readBoolean();
        return present ? in.readUTF() : null;
    }
}
