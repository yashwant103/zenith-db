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

            String rawData = new String(buffer.array(), 0, bytesRead).trim();
            if (rawData.isEmpty()) return;

            if (rawData.startsWith("{")) {
                try {
                    RaftMessage msg = MessageSerializer.fromJson(rawData);
                    raftNode.receiveMessage(msg);
                } catch (Exception e) {
                    System.out.println("⚠️ Failed to parse Raft message: " + e.getMessage());
                }
            } else {
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