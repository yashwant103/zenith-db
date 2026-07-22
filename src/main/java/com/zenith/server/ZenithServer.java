package com.zenith.server;

import com.zenith.metrics.ZenithMetrics;
import com.zenith.raft.RaftNode;
import com.zenith.raft.rpc.RaftMessage;
import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;
import com.zenith.wal.WriteAheadLog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class ZenithServer {

    private final MemoryEngine engine;
    private final WriteAheadLog log;
    private final RaftNode raftNode;
    private Selector selector;
    private final ZenithMetrics metrics;

    private static final int BUFFER_SIZE = 262144;

    public ZenithServer(MemoryEngine engine, WriteAheadLog log, RaftNode raftNode, ZenithMetrics metrics) {
        this.engine   = engine;
        this.log      = log;
        this.raftNode = raftNode;
        this.metrics = metrics;
    }

    public void start(int port) {
        try {
            selector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();

            // FIX: Explicitly bind to 0.0.0.0 so Docker exposes this to the host machine
            serverChannel.bind(new InetSocketAddress("0.0.0.0", port));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("🚀 ZenithDB NIO Server listening on 0.0.0.0:" + port + "...");

            while (true) {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter    = selectedKeys.iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if      (key.isAcceptable()) handleAccept(serverChannel);
                    else if (key.isReadable())   handleRead(key);
                    iter.remove();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleAccept(ServerSocketChannel serverChannel) throws IOException {
        SocketChannel client = serverChannel.accept();
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
    }

    /** Per-channel byte accumulator. Simple array-backed buffer supporting
     *  append-to-end and consume-from-front — message sizes here are modest
     *  (a handful of KB at most for a 50-entry AppendEntries batch), so a
     *  straightforward copy-based implementation is the right tradeoff:
     *  clear and correct over maximally performant. */
    private static class ByteAccumulator {
        private byte[] buf = new byte[0];

        void append(byte[] data, int len) {
            byte[] next = new byte[buf.length + len];
            System.arraycopy(buf, 0, next, 0, buf.length);
            System.arraycopy(data, 0, next, buf.length, len);
            buf = next;
        }

        int size() { return buf.length; }
        byte at(int i) { return buf[i]; }

        /** Returns the first `len` bytes and removes them from the front. */
        byte[] consume(int len) {
            byte[] result = new byte[len];
            System.arraycopy(buf, 0, result, 0, len);
            byte[] remainder = new byte[buf.length - len];
            System.arraycopy(buf, len, remainder, 0, remainder.length);
            buf = remainder;
            return result;
        }

        /** -1 if no newline (0x0A) found yet in the buffered bytes. */
        int indexOfNewline() {
            for (int i = 0; i < buf.length; i++) if (buf[i] == '\n') return i;
            return -1;
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer    = ByteBuffer.allocate(BUFFER_SIZE);

        // FIX: real message framing, now byte-level (not char-level) so it
        // can carry genuinely binary data — see BinaryMessageCodec. Two
        // message shapes are multiplexed on the same port:
        //   1. Binary Raft peer-to-peer RPCs — framed as
        //      [FRAME_MAGIC byte][4-byte length][binary payload]
        //   2. Plain-text client commands (human via `nc`, or ZenithClient) —
        //      newline-terminated, unchanged from before.
        // The first byte of a message unambiguously tells us which: every
        // legitimate text command starts with an uppercase ASCII letter
        // (INSERT/SELECT/UPDATE/DELETE/...), and FRAME_MAGIC (0x02, an
        // unprintable control byte) can never be the first byte of one.
        ByteAccumulator pending = (ByteAccumulator) key.attachment();
        if (pending == null) {
            pending = new ByteAccumulator();
            key.attach(pending);
        }

        try {
            int bytesRead = client.read(buffer);
            if (bytesRead == -1) {
                client.close();
                return;
            }
            if (bytesRead == 0) return;

            pending.append(buffer.array(), bytesRead);

            while (true) {
                if (pending.size() == 0) break;

                if (pending.at(0) == com.zenith.raft.rpc.BinaryMessageCodec.FRAME_MAGIC) {
                    if (pending.size() < 5) break; // need magic byte + 4-byte length first
                    int payloadLen =
                            ((pending.at(1) & 0xFF) << 24) |
                            ((pending.at(2) & 0xFF) << 16) |
                            ((pending.at(3) & 0xFF) << 8)  |
                            (pending.at(4) & 0xFF);
                    if (pending.size() < 5 + payloadLen) break; // wait for the rest of the payload

                    byte[] frame = pending.consume(5 + payloadLen);
                    byte[] payload = new byte[payloadLen];
                    System.arraycopy(frame, 5, payload, 0, payloadLen);
                    try {
                        RaftMessage msg = com.zenith.raft.rpc.BinaryMessageCodec.decode(payload);
                        raftNode.receiveMessage(msg);
                    } catch (Exception e) {
                        System.out.println("⚠️ Failed to decode binary Raft message: " + e.getMessage());
                    }

                } else {
                    int nl = pending.indexOfNewline();
                    if (nl == -1) break; // incomplete text command, wait for more data
                    byte[] lineBytes = pending.consume(nl + 1);
                    String line = new String(lineBytes, 0, nl, java.nio.charset.StandardCharsets.UTF_8).trim();
                    if (!line.isEmpty()) {
                        handleTextCommand(client, line);
                    }
                }
            }

        } catch (IOException e) {
            try { client.close(); } catch (IOException ignored) {}
        }
    }

    private void handleTextCommand(SocketChannel client, String rawData) {
        try {
            String response = processCommand(rawData);
            client.write(ByteBuffer.wrap((response + "\n").getBytes()));
        } catch (IOException e) {
            try { client.close(); } catch (IOException ignored) {}
        }
    }

    public String processCommand(String rawCommand) {
        String[] parts = rawCommand.split(",");

        try {
            switch (parts[0]) {
                case "SELECT" -> {
                    // FIX: previously any node — including a follower that may be
                    // lagging behind on replication — answered reads directly from
                    // its own local state. That was fixed by redirecting to the
                    // leader, then upgraded further: "I am currently the leader" on
                    // its own isn't a linearizability guarantee (a just-partitioned
                    // leader doesn't know it yet), so this now uses a leader-lease
                    // check — see RaftNode.isSafeForLinearizableRead() for the full
                    // reasoning and its stated tradeoffs vs. a full ReadIndex round-trip.
                    if (!raftNode.isSafeForLinearizableRead()) {
                        return "ERROR: Cannot guarantee a linearizable read right now " +
                                "(not leader, or leadership not recently confirmed by a majority). " +
                                "Retry shortly or query the LEADER directly.";
                    }
                    Trade t = engine.getTrade(parts[1]);
                    if (t != null) {
                        return "FOUND: " + t.tradeId() + " | " + t.tickerSymbol() +
                                " | Qty: " + t.amount() + " | $" + t.price() + " | " + t.status();
                    }
                    return "NOT_FOUND: Trade " + parts[1] + " does not exist.";
                }
                case "SELECT_TICKER" -> {
                    if (!raftNode.isSafeForLinearizableRead()) {
                        return "ERROR: Cannot guarantee a linearizable read right now " +
                                "(not leader, or leadership not recently confirmed by a majority). " +
                                "Retry shortly or query the LEADER directly.";
                    }
                    java.util.List<Trade> trades = engine.getTradesByTicker(parts[1]);
                    if (trades.isEmpty()) return "NOT_FOUND: No trades for ticker " + parts[1];
                    StringBuilder sb = new StringBuilder("FOUND " + trades.size() + " TRADES: ");
                    for (Trade t : trades) {
                        sb.append("[").append(t.tradeId())
                                .append(" | $").append(t.price())
                                .append(" | ").append(t.status()).append("] ");
                    }
                    return sb.toString();
                }
                case "INSERT", "UPDATE", "DELETE" -> {
                    String requestId = parts.length > 6 ? parts[6] : null;
                    if (requestId != null && engine.hasProcessed(requestId)) {
                        return "ALREADY_PROCESSED: " + requestId + " safely executed previously.";
                    }
                    if (!raftNode.isLeader()) {
                        return "ERROR: I am a FOLLOWER. Send trades to the active LEADER.";
                    }
                    raftNode.submitClientCommand(parts[0], parts[1], rawCommand);
                    return "PENDING: Trade submitted to Raft Consensus Cluster.";
                }
                case "COMPACT" -> {
                    log.compact(engine);
                    return "SUCCESS: Database compacted and optimized.";
                }
                default -> { return "ERROR: Unknown command: " + parts[0]; }
            }
        } catch (Exception e) {
            return "ERROR: Server fault — " + e.getMessage();
        }
    }
}