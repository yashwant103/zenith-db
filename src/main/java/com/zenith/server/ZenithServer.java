package com.zenith.server;

import com.zenith.metrics.ZenithMetrics;
import com.zenith.raft.RaftNode;
import com.zenith.raft.rpc.MessageSerializer;
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

/**
 * ZenithServer — NIO event loop + command orchestrator.
 *
 * FIXES FROM YOUR VERSION:
 *
 * BUG 1 — ByteBuffer.allocate(2048) too small for large AppendEntries JSON
 * A single AppendEntries with 10 log entries can easily exceed 2KB.
 * The buffer fills up, the JSON is truncated, MessageSerializer.fromJson()
 * throws an exception, and the Raft RPC is silently dropped.
 * Fix: increase to 65536 (64KB) — handles any realistic Raft message.
 *
 * BUG 2 — rawData = new String(buffer.array()).trim() reads null bytes
 * ByteBuffer.allocate() fills the buffer with 0x00 bytes. After reading N bytes,
 * the remaining (65536 - N) bytes are all null characters. new String(buffer.array())
 * includes all of them. trim() only removes leading/trailing whitespace — not null chars.
 * The JSON parser then fails on the embedded null bytes.
 * Fix: use new String(buffer.array(), 0, bytesRead) to read only actual bytes.
 *
 * BUG 3 — ZenithServer handles SELECT directly from MemoryEngine
 * SELECT bypasses Raft entirely — correct for reads in a CP system where
 * follower reads are acceptable. But the check `raftNode != null` is fragile.
 * Minor: raftNode is always non-null after construction — simplified check.
 */
public class ZenithServer {

    private final MemoryEngine engine;
    private final WriteAheadLog log;
    private final RaftNode raftNode;
    private Selector selector;
    private final ZenithMetrics metrics;

    // FIX BUG 1: 64KB buffer handles any realistic Raft JSON message
    private static final int    BUFFER_SIZE = 65536;

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
            serverChannel.bind(new InetSocketAddress(port));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("🚀 ZenithDB NIO Server listening on port " + port + "...");

            while (true) {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter    = selectedKeys.iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if      (key.isAcceptable()) handleAccept(serverChannel);
                    else if (key.isReadable())   handleRead(key);
                    iter.remove(); // CRITICAL: must remove or event fires forever
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

    private void handleRead(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer    = ByteBuffer.allocate(BUFFER_SIZE);

        try {
            int bytesRead = client.read(buffer);
            if (bytesRead == -1) {
                client.close();
                return;
            }
            if (bytesRead == 0) return;

            // FIX BUG 2: read exactly bytesRead bytes — not the entire null-padded buffer
            String rawData = new String(buffer.array(), 0, bytesRead).trim();
            if (rawData.isEmpty()) return;

            if (rawData.startsWith("{")) {
                // Raft RPC from another node (JSON)
                try {
                    RaftMessage msg = MessageSerializer.fromJson(rawData);
                    raftNode.receiveMessage(msg);
                } catch (Exception e) {
                    System.out.println("⚠️ Failed to parse Raft message: " + e.getMessage());
                }
            } else {
                // Plain-text client command
                String response = processCommand(rawData);
                client.write(ByteBuffer.wrap((response + "\n").getBytes()));
            }

        } catch (IOException e) {
            try { client.close(); } catch (IOException ignored) {}
        }
    }

    public String processCommand(String rawCommand) {
        String[] parts = rawCommand.split(",");

        try {
            switch (parts[0]) {

                case "SELECT" -> {
                    Trade t = engine.getTrade(parts[1]);
                    if (t != null) {
                        return "FOUND: " + t.tradeId() + " | " + t.tickerSymbol() +
                                " | Qty: " + t.amount() + " | $" + t.price() + " | " + t.status();
                    }
                    return "NOT_FOUND: Trade " + parts[1] + " does not exist.";
                }

                case "SELECT_TICKER" -> {
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
                    // Idempotency check at gateway
                    String requestId = parts.length > 6 ? parts[6] : null;
                    if (requestId != null && engine.hasProcessed(requestId)) {
                        return "ALREADY_PROCESSED: " + requestId + " safely executed previously.";
                    }

                    // FIX BUG 3: simplified null check
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