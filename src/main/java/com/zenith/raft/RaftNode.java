//package com.zenith.raft;
//
//import com.zenith.metrics.ZenithMetrics;
//import com.zenith.raft.rpc.*;
//import com.zenith.raft.state.RaftRole;
//import com.zenith.raft.state.RaftState;
//import com.zenith.storage.MemoryEngine;
//import com.zenith.storage.Trade;
//import com.zenith.wal.WriteAheadLog;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//import java.util.concurrent.*;
//
//public class RaftNode {
//
//    private final String         nodeId;
//    private final List<String>   peers;
//    private final RaftState      state;
//    private final MemoryEngine   engine;
//    private final WriteAheadLog  wal;
//    private final ZenithMetrics  metrics; // METRICS ENGINE ADDED
//
//    private static final int SNAPSHOT_THRESHOLD = 5;
//
//    private final BlockingQueue<RaftMessage> inbox = new LinkedBlockingQueue<>();
//    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
//    private final ExecutorService rpcPool = Executors.newCachedThreadPool();
//
//    private ScheduledFuture<?> electionTimer;
//    private ScheduledFuture<?> heartbeatTimer;
//    private final Random random = new Random();
//
//    private int votesReceived = 0;
//    private final Map<String, Integer> nextIndex  = new ConcurrentHashMap<>();
//    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();
//
//    public RaftNode(String nodeId, List<String> peers, RaftState state,
//                    MemoryEngine engine, WriteAheadLog wal, ZenithMetrics metrics) {
//        this.nodeId = nodeId;
//        this.peers  = peers;
//        this.state  = state;
//        this.engine = engine;
//        this.wal    = wal;
//        this.metrics = metrics; // SAVED
//    }
//
//    public void start() {
//        System.out.println("🚀 Node [" + nodeId + "] booting up as FOLLOWER...");
//        metrics.setLeader(false);
//        metrics.setRaftTerm(state.getCurrentTerm());
//        resetElectionTimer();
//
//        Thread coreThread = new Thread(() -> {
//            try {
//                while (true) {
//                    RaftMessage message = inbox.take();
//                    processMessage(message);
//                }
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                System.out.println("🛑 Node [" + nodeId + "] shutting down.");
//            }
//        });
//        coreThread.setName("RaftCore-" + nodeId);
//        coreThread.setDaemon(true);
//        coreThread.start();
//    }
//
//    public void receiveMessage(RaftMessage message) {
//        inbox.offer(message);
//    }
//
//    private void processMessage(RaftMessage message) {
//        if      (message instanceof TriggerElectionCommand)  handleTriggerElection();
//        else if (message instanceof TriggerHeartbeatCommand) handleTriggerHeartbeat();
//        else if (message instanceof VoteRequest  r)          handleVoteRequest(r);
//        else if (message instanceof VoteResponse r)          handleVoteResponse(r);
//        else if (message instanceof AppendEntriesRequest  r) handleAppendEntriesRequest(r);
//        else if (message instanceof AppendEntriesResponse r) handleAppendEntriesResponse(r);
//        else System.out.println("⚠️ Node [" + nodeId + "] ignored unknown: " + message.getClass().getSimpleName());
//    }
//
//    // ── Election timer ──
//    private void resetElectionTimer() {
//        if (electionTimer != null && !electionTimer.isDone()) electionTimer.cancel(false);
//        int timeoutMs = 1500 + random.nextInt(1500);
//        electionTimer = scheduler.schedule(this::startElection, timeoutMs, TimeUnit.MILLISECONDS);
//    }
//
//    private void startElection() {
//        inbox.offer(new TriggerElectionCommand());
//    }
//
//    private void handleTriggerElection() {
//        if (state.getRole() == RaftRole.LEADER) return;
//
//        System.out.println("\n⏰ Node [" + nodeId + "] ELECTION TIMEOUT! Starting election.");
//        state.convertToCandidate();
//
//        // --- METRICS UPDATE ---
//        metrics.recordElection();
//        metrics.setRaftTerm(state.getCurrentTerm());
//        metrics.setLeader(false);
//        // ----------------------
//
//        votesReceived = 1;
//        state.setVotedFor(nodeId);
//
//        for (String peerId : peers) {
//            VoteRequest req = new VoteRequest(
//                    nodeId,
//                    state.getCurrentTerm(),
//                    state.getLog().getLastLogIndex(),
//                    state.getLog().getLastLogTerm()
//            );
//            sendToPeer(peerId, req);
//        }
//        resetElectionTimer();
//    }
//
//    private void handleVoteRequest(VoteRequest req) {
//        if (req.getTerm() > state.getCurrentTerm()) {
//            state.convertToFollower(req.getTerm());
//            metrics.setRaftTerm(req.getTerm()); // METRICS UPDATE
//            metrics.setLeader(false);           // METRICS UPDATE
//        }
//
//        boolean grantVote = false;
//
//        if (req.getTerm() == state.getCurrentTerm() &&
//                (state.getVotedFor() == null || state.getVotedFor().equals(req.getSenderId()))) {
//
//            boolean candidateLogIsFresh =
//                    req.getLastLogTerm() > state.getLog().getLastLogTerm() ||
//                            (req.getLastLogTerm() == state.getLog().getLastLogTerm() &&
//                                    req.getLastLogIndex() >= state.getLog().getLastLogIndex());
//
//            if (candidateLogIsFresh) {
//                grantVote = true;
//                state.setVotedFor(req.getSenderId());
//                resetElectionTimer();
//                System.out.println("🗳️ Node [" + nodeId + "] voted YES for " + req.getSenderId());
//            } else {
//                System.out.println("❌ Node [" + nodeId + "] voted NO (stale log) for " + req.getSenderId());
//            }
//        } else {
//            System.out.println("❌ Node [" + nodeId + "] voted NO (already voted) for " + req.getSenderId());
//        }
//
//        VoteResponse resp = new VoteResponse(nodeId, state.getCurrentTerm(), grantVote);
//        sendToPeer(req.getSenderId(), resp);
//    }
//
//    private void handleVoteResponse(VoteResponse resp) {
//        if (state.getRole() != RaftRole.CANDIDATE) return;
//
//        if (resp.getTerm() > state.getCurrentTerm()) {
//            state.convertToFollower(resp.getTerm());
//            metrics.setRaftTerm(resp.getTerm()); // METRICS UPDATE
//            resetElectionTimer();
//            return;
//        }
//
//        if (resp.isVoteGranted()) {
//            votesReceived++;
//            System.out.println("✅ Node [" + nodeId + "] received vote from " +
//                    resp.getSenderId() + " (Total: " + votesReceived + ")");
//
//            int majority = (peers.size() + 1) / 2 + 1;
//            if (votesReceived >= majority) {
//                state.convertToLeader();
//
//                // --- METRICS UPDATE ---
//                metrics.setLeader(true);
//                // ----------------------
//
//                System.out.println("👑 Node [" + nodeId + "] IS NOW LEADER for term " +
//                        state.getCurrentTerm() + "!");
//
//                if (electionTimer != null) electionTimer.cancel(false);
//
//                for (String peer : peers) {
//                    nextIndex.put(peer,  state.getLog().getLastLogIndex() + 1);
//                    matchIndex.put(peer, -1);
//                }
//                startHeartbeatBroadcast();
//            }
//        }
//    }
//
//    // ── Heartbeat ──
//    private void startHeartbeatBroadcast() {
//        if (heartbeatTimer != null && !heartbeatTimer.isDone()) heartbeatTimer.cancel(false);
//        heartbeatTimer = scheduler.scheduleAtFixedRate(
//                this::broadcastAppendEntries, 0, 500, TimeUnit.MILLISECONDS
//        );
//    }
//
//    private void broadcastAppendEntries() {
//        inbox.offer(new TriggerHeartbeatCommand());
//    }
//
//    private void handleTriggerHeartbeat() {
//        if (state.getRole() != RaftRole.LEADER) {
//            if (heartbeatTimer != null) heartbeatTimer.cancel(false);
//            return;
//        }
//
//        int leaderLastIndex = state.getLog().getLastLogIndex();
//
//        for (String peerId : peers) {
//            int peerNextIndex  = nextIndex.getOrDefault(peerId, leaderLastIndex + 1);
//            List<AppendEntriesRequest.LogEntryDTO> missing = state.getLog().getEntriesFrom(peerNextIndex);
//
//            int prevLogIndex = peerNextIndex - 1;
//            int prevLogTerm  = 0;
//            if (prevLogIndex >= 0) {
//                AppendEntriesRequest.LogEntryDTO prev = state.getLog().getEntry(prevLogIndex);
//                if (prev != null) prevLogTerm = prev.getTerm();
//            }
//
//            AppendEntriesRequest req = new AppendEntriesRequest(
//                    nodeId, state.getCurrentTerm(),
//                    prevLogIndex, prevLogTerm,
//                    missing,
//                    state.getLog().getCommitIndex()
//            );
//            sendToPeer(peerId, req);
//        }
//    }
//
//    // ── AppendEntries ──
//    private void handleAppendEntriesRequest(AppendEntriesRequest req) {
//        if (req.getTerm() < state.getCurrentTerm()) {
//            sendToPeer(req.getSenderId(),
//                    AppendEntriesResponse.staleLeader(nodeId, state.getCurrentTerm()));
//            return;
//        }
//
//        if (req.getTerm() > state.getCurrentTerm() || state.getRole() != RaftRole.FOLLOWER) {
//            state.convertToFollower(req.getTerm());
//            metrics.setRaftTerm(req.getTerm()); // METRICS UPDATE
//            metrics.setLeader(false);           // METRICS UPDATE
//        }
//        state.setCurrentLeaderId(req.getSenderId());
//        resetElectionTimer();
//
//        int prevLogIndex = req.getPrevLogIndex();
//        if (prevLogIndex >= 0) {
//            AppendEntriesRequest.LogEntryDTO localPrev = state.getLog().getEntry(prevLogIndex);
//            if (localPrev == null || localPrev.getTerm() != req.getPrevLogTerm()) {
//                System.out.println("   [Follower " + nodeId + "] ⚠️ Log mismatch at index " +
//                        prevLogIndex + ". Rejecting.");
//                sendToPeer(req.getSenderId(),
//                        AppendEntriesResponse.rejected(nodeId, state.getCurrentTerm(),
//                                state.getLog().getLastLogIndex()));
//                return;
//            }
//        }
//
//        if (!req.isHeartbeat()) {
//            state.getLog().appendEntriesFrom(prevLogIndex, req.getEntries());
//            System.out.println("   [Follower " + nodeId + "] 💾 Appended " +
//                    req.getEntries().size() + " entries. Log now at index " +
//                    state.getLog().getLastLogIndex());
//        }
//
//        if (req.getLeaderCommit() > state.getLog().getCommitIndex()) {
//            state.getLog().setCommitIndex(
//                    Math.min(req.getLeaderCommit(), state.getLog().getLastLogIndex())
//            );
//            applyCommittedEntries();
//        }
//
//        sendToPeer(req.getSenderId(),
//                AppendEntriesResponse.accepted(nodeId, state.getCurrentTerm(),
//                        state.getLog().getLastLogIndex()));
//    }
//
//    private void handleAppendEntriesResponse(AppendEntriesResponse resp) {
//        if (state.getRole() != RaftRole.LEADER) return;
//
//        if (resp.getTerm() > state.getCurrentTerm()) {
//            state.convertToFollower(resp.getTerm());
//            metrics.setRaftTerm(resp.getTerm()); // METRICS UPDATE
//            metrics.setLeader(false);           // METRICS UPDATE
//            resetElectionTimer();
//            return;
//        }
//
//        if (resp.isSuccess()) {
//            matchIndex.put(resp.getSenderId(), resp.getMatchIndex());
//            nextIndex.put(resp.getSenderId(),  resp.getMatchIndex() + 1);
//            advanceCommitIndex();
//        } else {
//            int currentNext = nextIndex.getOrDefault(resp.getSenderId(), 1);
//            int baseIndex   = state.getLog().getBaseIndex();
//            nextIndex.put(resp.getSenderId(), Math.max(baseIndex, currentNext - 1));
//        }
//    }
//
//    private void advanceCommitIndex() {
//        int majority = (peers.size() + 1) / 2 + 1;
//
//        for (int n = state.getLog().getLastLogIndex(); n > state.getLog().getCommitIndex(); n--) {
//            int replicationCount = 1;
//
//            for (int mIdx : matchIndex.values()) {
//                if (mIdx >= n) replicationCount++;
//            }
//
//            if (replicationCount >= majority) {
//                state.getLog().setCommitIndex(n);
//                AppendEntriesRequest.LogEntryDTO committed = state.getLog().getEntry(n);
//                System.out.println("✅ [LEADER " + nodeId + "] COMMITTED: " +
//                        committed.getOperation() + " " + committed.getKey());
//                applyCommittedEntries();
//                break;
//            }
//        }
//    }
//
//    private void applyCommittedEntries() {
//        while (state.getLog().getLastApplied() < state.getLog().getCommitIndex()) {
//            int idx = state.getLog().getLastApplied() + 1;
//            AppendEntriesRequest.LogEntryDTO entry = state.getLog().getEntry(idx);
//
//            if (entry != null && !"INTERNAL_HB".equals(entry.getOperation())) {
//                executeOnStateMachine(entry);
//                state.getLog().setLastApplied(idx);
//            } else {
//                break;
//            }
//        }
//
//        int activeLogSize = state.getLog().getLastLogIndex() - state.getLog().getBaseIndex();
//        if (activeLogSize >= SNAPSHOT_THRESHOLD) {
//            System.out.println("\n📦 [SNAPSHOT] Compacting log...");
//            wal.compact(engine);
//            state.getLog().compactLog(state.getLog().getLastApplied());
//
//            int newBase = state.getLog().getBaseIndex();
//            for (String peer : peers) {
//                if (nextIndex.getOrDefault(peer, 0) < newBase) {
//                    nextIndex.put(peer, newBase);
//                }
//            }
//
//            String hash = com.zenith.storage.StateHasher.generateStateHash(engine);
//            System.out.println("🔐 [ANTI-ENTROPY] Hash: " + hash.substring(0, 16) + "...");
//        }
//    }
//
//    private void executeOnStateMachine(AppendEntriesRequest.LogEntryDTO entry) {
//        long startTime = System.nanoTime(); // Latency tracking start
//
//        String rawCommand = entry.getValue();
//        String[] parts    = rawCommand.split(",");
//
//        try {
//            String requestId = parts.length > 6 ? parts[6] : null;
//            if (requestId != null) {
//                if (engine.hasProcessed(requestId)) {
//                    System.out.println("♻️ [IDEMPOTENCY " + nodeId + "] Blocked duplicate: " + requestId);
//                    metrics.recordDuplicate(); // METRICS UPDATE
//                    return;
//                }
//                engine.markProcessed(requestId);
//            }
//
//            if ("INSERT".equals(parts[0])) {
//                Trade t = new Trade(parts[1], parts[2],
//                        Integer.parseInt(parts[3]), Double.parseDouble(parts[4]), parts[5]);
//                wal.appendTrade(t);
//                metrics.recordWalFlush(); // METRICS UPDATE
//                engine.insertTrade(t);
//                metrics.recordTrade();    // METRICS UPDATE
//            } else if ("UPDATE".equals(parts[0])) {
//                if (engine.updateTradeStatus(parts[1], parts[2])) {
//                    wal.appendTrade(engine.getTrade(parts[1]));
//                    metrics.recordWalFlush(); // METRICS UPDATE
//                }
//            } else if ("DELETE".equals(parts[0])) {
//                if (engine.deleteTrade(parts[1])) {
//                    wal.appendTrade(new Trade(parts[1], "DELETED", 0, 0.0, "DELETED"));
//                    metrics.recordWalFlush(); // METRICS UPDATE
//                }
//            }
//            System.out.println("💾 [STATE MACHINE " + nodeId + "] Applied: " + parts[1]);
//
//            // --- METRICS UPDATE ---
//            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
//            metrics.recordLatency(durationMs);
//            // ----------------------
//
//        } catch (Exception e) {
//            System.out.println("❌ [STATE MACHINE] Failed to apply: " + e.getMessage());
//        }
//    }
//
//    public void submitClientCommand(String operation, String key, String value) {
//        if (state.getRole() != RaftRole.LEADER) {
//            System.out.println("❌ Node [" + nodeId + "] is not Leader — redirect client.");
//            return;
//        }
//
//        int newIndex = state.getLog().getLastLogIndex() + 1;
//        AppendEntriesRequest.LogEntryDTO entry = new AppendEntriesRequest.LogEntryDTO(
//                newIndex, state.getCurrentTerm(), key, value, operation, System.nanoTime()
//        );
//
//        state.getLog().appendEntry(entry);
//        System.out.println("\n📝 [LEADER " + nodeId + "] Appended to log[" + newIndex + "]: " +
//                operation + " " + key);
//        broadcastAppendEntries();
//    }
//
//    public boolean isLeader() {
//        return state.getRole() == RaftRole.LEADER;
//    }
//
//    private static class TriggerHeartbeatCommand extends RaftMessage {
//        public TriggerHeartbeatCommand() { super("INTERNAL_HB", -1); }
//    }
//    private static class TriggerElectionCommand extends RaftMessage {
//        public TriggerElectionCommand() { super("INTERNAL", -1); }
//    }
//
//    private void sendToPeer(String peerId, RaftMessage message) {
//        if (!(message instanceof AppendEntriesRequest req && req.isHeartbeat()) &&
//                !(message instanceof AppendEntriesResponse)) {
//            System.out.println("   [Network] " + nodeId + " → " + peerId +
//                    " : " + message.getClass().getSimpleName());
//        }
//
//        // Removed the ClusterSimulator check here to keep it strictly Docker/TCP based as requested
//        // If you need it back for local unit tests, just uncomment your old ClusterSimulator block!
//
//        if (!peerId.contains(":")) {
//            System.out.println("⚠️ Cannot route to peer: " + peerId + " (no host:port)");
//            return;
//        }
//
//        rpcPool.submit(() -> {
//            String[] parts = peerId.split(":");
//            String host    = parts[0];
//            int port       = Integer.parseInt(parts[1]);
//
//            try (java.net.Socket socket = new java.net.Socket(host, port);
//                 java.io.PrintWriter out =
//                         new java.io.PrintWriter(socket.getOutputStream(), true)) {
//                socket.setSoTimeout(500);
//                out.println(MessageSerializer.toJson(message));
//            } catch (java.io.IOException ignored) {
//            }
//        });
//    }
//}


package com.zenith.raft;

import com.zenith.metrics.ZenithMetrics;
import com.zenith.raft.rpc.*;
import com.zenith.raft.state.RaftRole;
import com.zenith.raft.state.RaftState;
import com.zenith.storage.MemoryEngine;
import com.zenith.storage.Trade;
import com.zenith.wal.WriteAheadLog;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.Set;

public class RaftNode {

    private final String        nodeId;
    private final List<String>  peers;
    private final RaftState     state;
    private final MemoryEngine  engine;
    private final WriteAheadLog wal;
    private final ZenithMetrics metrics;

    private static final int SNAPSHOT_THRESHOLD = 5;

    private final BlockingQueue<RaftMessage> inbox    = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService scheduler  = Executors.newScheduledThreadPool(2);
    private final ExecutorService rpcPool             = Executors.newCachedThreadPool();

    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;
    private final Random random = new Random();

    // Each server's vote should only be counted once per election.
    private final Set<String> grantedVotes = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> nextIndex  = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    public RaftNode(String nodeId, List<String> peers, RaftState state,
                    MemoryEngine engine, WriteAheadLog wal, ZenithMetrics metrics) {
        this.nodeId  = nodeId;
        this.peers   = peers;
        this.state   = state;
        this.engine  = engine;
        this.wal     = wal;
        this.metrics = metrics;
    }

    public void start() {
        System.out.println("🚀 Node [" + nodeId + "] booting up as FOLLOWER...");
        metrics.setLeader(false);
        metrics.setRaftTerm(state.getCurrentTerm());
        resetElectionTimer();

        Thread coreThread = new Thread(() -> {
            try {
                while (running) {
                    RaftMessage message = inbox.take();
                    processMessage(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("🛑 Node [" + nodeId + "] shutting down.");
            }
        });
        coreThread.setName("RaftCore-" + nodeId);
        coreThread.setDaemon(true);
        coreThread.start();
    }

    public void receiveMessage(RaftMessage message) { inbox.offer(message); }

    private void processMessage(RaftMessage message) {
        if      (message instanceof TriggerElectionCommand)  handleTriggerElection();
        else if (message instanceof TriggerHeartbeatCommand) handleTriggerHeartbeat();
        else if (message instanceof VoteRequest  r)          handleVoteRequest(r);
        else if (message instanceof VoteResponse r)          handleVoteResponse(r);
        else if (message instanceof AppendEntriesRequest  r) handleAppendEntriesRequest(r);
        else if (message instanceof AppendEntriesResponse r) handleAppendEntriesResponse(r);
        else System.out.println("⚠️ Unknown message: " + message.getClass().getSimpleName());
    }

//    private void resetElectionTimer() {
//        if (electionTimer != null && !electionTimer.isDone()) electionTimer.cancel(false);
//        int timeoutMs = 1500 + random.nextInt(1500);
//        electionTimer = scheduler.schedule(this::startElection, timeoutMs, TimeUnit.MILLISECONDS);
//    }
private void resetElectionTimer() {
    if (electionTimer != null && !electionTimer.isDone()) electionTimer.cancel(false);
    // FIX: Increased timeout from 1500-3000ms to 4000-6000ms for Windows Docker
    // Windows Docker Desktop adds 200-400ms container-to-container latency per hop.
    // At 1500ms timeout + 500ms heartbeat interval, heartbeats were arriving just
    // barely in time — any GC pause or network blip caused a false election.
    // 4000-6000ms gives 8-12x heartbeat intervals of buffer before triggering election.
    // Raft paper recommends: electionTimeout = 10x heartbeatInterval minimum.
    // Our heartbeat = 500ms, so minimum election timeout = 5000ms.
    int timeoutMs = 4000 + random.nextInt(2000);
    electionTimer = scheduler.schedule(this::startElection, timeoutMs, TimeUnit.MILLISECONDS);
}

    private void startElection() { inbox.offer(new TriggerElectionCommand()); }

    private void handleTriggerElection() {
        if (state.getRole() == RaftRole.LEADER) return;
        System.out.println("\n⏰ Node [" + nodeId + "] ELECTION TIMEOUT! Starting election.");
        state.convertToCandidate();
        metrics.recordElection();
        metrics.setRaftTerm(state.getCurrentTerm());
        metrics.setLeader(false);

//        votesReceived = 1;
//        state.setVotedFor(nodeId);

        grantedVotes.clear();
// Candidate always votes for itself.
        grantedVotes.add(nodeId);
        state.setVotedFor(nodeId);

        for (String peerId : peers) {
            sendToPeer(peerId, new VoteRequest(nodeId, state.getCurrentTerm(),
                    state.getLog().getLastLogIndex(), state.getLog().getLastLogTerm()));
        }
        resetElectionTimer();
    }

    private void handleVoteRequest(VoteRequest req) {
        if (req.getTerm() > state.getCurrentTerm()) {
            state.convertToFollower(req.getTerm());
            metrics.setRaftTerm(req.getTerm());
            metrics.setLeader(false);
        }

        boolean grantVote = false;
        if (req.getTerm() == state.getCurrentTerm() &&
                (state.getVotedFor() == null || state.getVotedFor().equals(req.getSenderId()))) {
            boolean fresh = req.getLastLogTerm() > state.getLog().getLastLogTerm() ||
                    (req.getLastLogTerm() == state.getLog().getLastLogTerm() &&
                            req.getLastLogIndex() >= state.getLog().getLastLogIndex());
            if (fresh) {
                grantVote = true;
                state.setVotedFor(req.getSenderId());
                resetElectionTimer();
                System.out.println("🗳️ Node [" + nodeId + "] voted YES for " + req.getSenderId());
            }
        }
        sendToPeer(req.getSenderId(), new VoteResponse(nodeId, state.getCurrentTerm(), grantVote));
    }

    private void handleVoteResponse(VoteResponse resp) {
        if (state.getRole() != RaftRole.CANDIDATE) return;
        if (resp.getTerm() > state.getCurrentTerm()) {
            state.convertToFollower(resp.getTerm());
            metrics.setRaftTerm(resp.getTerm());
            resetElectionTimer();
            return;
        }
        if (resp.isVoteGranted()) {

            // add() returns false if we've already counted this node's vote.
            if (grantedVotes.add(resp.getSenderId())) {

                System.out.println(
                        "✅ Node [" + nodeId + "] received vote from "
                                + resp.getSenderId()
                                + " (Total: " + grantedVotes.size() + ")"
                );

                int majority = (peers.size() + 1) / 2 + 1;

                if (grantedVotes.size() >= majority) {

                    state.convertToLeader();

                    metrics.setLeader(true);

                    System.out.println( "👑 Node [" + nodeId + "] IS NOW LEADER for term "
                                    + state.getCurrentTerm() + "!"
                    );
                    if (electionTimer != null)
                        electionTimer.cancel(false);

                    for (String peer : peers) {
                        nextIndex.put(peer,
                                state.getLog().getLastLogIndex() + 1);
                        matchIndex.put(peer, -1);
                    }

                    startHeartbeatBroadcast();
                }
            }
        }
    }

    private void startHeartbeatBroadcast() {
        if (heartbeatTimer != null && !heartbeatTimer.isDone()) heartbeatTimer.cancel(false);
//        heartbeatTimer = scheduler.scheduleAtFixedRate(
//                this::broadcastAppendEntries, 0, 500, TimeUnit.MILLISECONDS);

        heartbeatTimer = scheduler.scheduleAtFixedRate(
                this::broadcastAppendEntries, 0, 200, TimeUnit.MILLISECONDS);
    }

    private void broadcastAppendEntries() { inbox.offer(new TriggerHeartbeatCommand()); }

    private void handleTriggerHeartbeat() {
        if (state.getRole() != RaftRole.LEADER) {
            if (heartbeatTimer != null) heartbeatTimer.cancel(false);
            return;
        }
        int leaderLastIndex = state.getLog().getLastLogIndex();
        for (String peerId : peers) {
            int peerNextIndex = nextIndex.getOrDefault(peerId, leaderLastIndex + 1);
            List<AppendEntriesRequest.LogEntryDTO> missing = state.getLog().getEntriesFrom(peerNextIndex);
            int prevLogIndex = peerNextIndex - 1;
            int prevLogTerm  = 0;
            if (prevLogIndex >= 0) {
                AppendEntriesRequest.LogEntryDTO prev = state.getLog().getEntry(prevLogIndex);
                if (prev != null) prevLogTerm = prev.getTerm();
            }
            sendToPeer(peerId, new AppendEntriesRequest(nodeId, state.getCurrentTerm(),
                    prevLogIndex, prevLogTerm, missing, state.getLog().getCommitIndex()));
        }
    }

    private void handleAppendEntriesRequest(AppendEntriesRequest req) {
        if (req.getTerm() < state.getCurrentTerm()) {
            sendToPeer(req.getSenderId(), AppendEntriesResponse.staleLeader(nodeId, state.getCurrentTerm()));
            return;
        }
        if (req.getTerm() > state.getCurrentTerm() || state.getRole() != RaftRole.FOLLOWER) {
            state.convertToFollower(req.getTerm());
            metrics.setRaftTerm(req.getTerm());
            metrics.setLeader(false);
        }
        state.setCurrentLeaderId(req.getSenderId());
        resetElectionTimer();

        int prevLogIndex = req.getPrevLogIndex();
        if (prevLogIndex >= 0) {
            AppendEntriesRequest.LogEntryDTO localPrev = state.getLog().getEntry(prevLogIndex);
            if (localPrev == null || localPrev.getTerm() != req.getPrevLogTerm()) {
                sendToPeer(req.getSenderId(), AppendEntriesResponse.rejected(nodeId,
                        state.getCurrentTerm(), state.getLog().getLastLogIndex()));
                return;
            }
        }

        if (!req.isHeartbeat()) {
            state.getLog().appendEntriesFrom(prevLogIndex, req.getEntries());
            System.out.println("   [Follower " + nodeId + "] 💾 Appended " +
                    req.getEntries().size() + " entries.");
        }
        if (req.getLeaderCommit() > state.getLog().getCommitIndex()) {
            state.getLog().setCommitIndex(
                    Math.min(req.getLeaderCommit(), state.getLog().getLastLogIndex()));
            applyCommittedEntries();
        }
        sendToPeer(req.getSenderId(), AppendEntriesResponse.accepted(nodeId,
                state.getCurrentTerm(), state.getLog().getLastLogIndex()));
    }

    private void handleAppendEntriesResponse(AppendEntriesResponse resp) {
        if (state.getRole() != RaftRole.LEADER) return;
        if (resp.getTerm() > state.getCurrentTerm()) {
            state.convertToFollower(resp.getTerm());
            metrics.setRaftTerm(resp.getTerm());
            metrics.setLeader(false);
            resetElectionTimer();
            return;
        }
        if (resp.isSuccess()) {
            matchIndex.put(resp.getSenderId(), resp.getMatchIndex());
            nextIndex.put(resp.getSenderId(),  resp.getMatchIndex() + 1);
            advanceCommitIndex();
        } else {
            int cur  = nextIndex.getOrDefault(resp.getSenderId(), 1);
            int base = state.getLog().getBaseIndex();
            nextIndex.put(resp.getSenderId(), Math.max(base, cur - 1));
        }
    }

    private void advanceCommitIndex() {
        int majority = (peers.size() + 1) / 2 + 1;
        for (int n = state.getLog().getLastLogIndex(); n > state.getLog().getCommitIndex(); n--) {
            int count = 1;
            for (int m : matchIndex.values()) if (m >= n) count++;
            if (count >= majority) {
                state.getLog().setCommitIndex(n);
                AppendEntriesRequest.LogEntryDTO committed = state.getLog().getEntry(n);
                System.out.println("✅ [LEADER " + nodeId + "] COMMITTED: " +
                        committed.getOperation() + " " + committed.getKey());
                applyCommittedEntries();
                break;
            }
        }
    }

    private void applyCommittedEntries() {
        while (state.getLog().getLastApplied() < state.getLog().getCommitIndex()) {
            int idx = state.getLog().getLastApplied() + 1;
            AppendEntriesRequest.LogEntryDTO entry = state.getLog().getEntry(idx);
            if (entry != null && !"INTERNAL_HB".equals(entry.getOperation())) {
                executeOnStateMachine(entry);
                state.getLog().setLastApplied(idx);
            } else break;
        }

        int activeLogSize = state.getLog().getLastLogIndex() - state.getLog().getBaseIndex();
        if (activeLogSize >= SNAPSHOT_THRESHOLD) {
            System.out.println("\n📦 [SNAPSHOT] Compacting log...");
            wal.compact(engine);
            state.getLog().compactLog(state.getLog().getLastApplied());
            int newBase = state.getLog().getBaseIndex();
            for (String peer : peers) {
                if (nextIndex.getOrDefault(peer, 0) < newBase) nextIndex.put(peer, newBase);
            }
            String hash = com.zenith.storage.StateHasher.generateStateHash(engine);
            System.out.println("🔐 [ANTI-ENTROPY] Hash: " + hash.substring(0, 16) + "...");
        }
    }

    private void executeOnStateMachine(AppendEntriesRequest.LogEntryDTO entry) {
        long startTime = System.nanoTime();
        String rawCommand = entry.getValue();
        String[] parts    = rawCommand.split(",");

        try {
            String requestId = parts.length > 6 ? parts[6] : null;
            if (requestId != null) {
                if (engine.hasProcessed(requestId)) {
                    System.out.println("♻️ [IDEMPOTENCY " + nodeId + "] Blocked duplicate: " + requestId);
                    metrics.recordDuplicate();
                    return;
                }
                engine.markProcessed(requestId);
            }

            if ("INSERT".equals(parts[0])) {
                Trade t = new Trade(parts[1], parts[2],
                        Integer.parseInt(parts[3]), Double.parseDouble(parts[4]), parts[5]);
                wal.appendTrade(t);
                engine.insertTrade(t);
                metrics.recordTrade();
                metrics.activeTrades.set(engine.size());
            } else if ("UPDATE".equals(parts[0])) {
                if (engine.updateTradeStatus(parts[1], parts[2])) {
                    wal.appendTrade(engine.getTrade(parts[1]));
                }
            } else if ("DELETE".equals(parts[0])) {
                if (engine.deleteTrade(parts[1])) {
                    wal.appendTrade(new Trade(parts[1], "DELETED", 0, 0.0, "DELETED"));
                    // FIX: decrement activeTrades gauge on delete
                    metrics.activeTrades.set(engine.size());
                }
            }

            System.out.println("💾 [STATE MACHINE " + nodeId + "] Applied: " + parts[1]);
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            metrics.recordLatency(durationMs);

        } catch (Exception e) {
            System.out.println("❌ [STATE MACHINE] Failed: " + e.getMessage());
        }
    }

    // FIX: removed metrics.recordWalFlush() from here — appendTrade() only buffers,
    // actual disk write is async. WAL flush counter now updated inside WriteAheadLog.flush()
    // by passing metrics reference to WAL (see WriteAheadLog fix).

    public void submitClientCommand(String operation, String key, String value) {
        if (state.getRole() != RaftRole.LEADER) {
            System.out.println("❌ Node [" + nodeId + "] is not Leader — redirect client.");
            return;
        }
        int newIndex = state.getLog().getLastLogIndex() + 1;
        AppendEntriesRequest.LogEntryDTO entry = new AppendEntriesRequest.LogEntryDTO(
                newIndex, state.getCurrentTerm(), key, value, operation, System.nanoTime());
        state.getLog().appendEntry(entry);
        System.out.println("\n📝 [LEADER " + nodeId + "] Appended to log[" + newIndex + "]: " + operation + " " + key);
        broadcastAppendEntries();
    }

    public boolean isLeader() { return state.getRole() == RaftRole.LEADER; }

    private static class TriggerHeartbeatCommand extends RaftMessage {
        public TriggerHeartbeatCommand() { super("INTERNAL_HB", -1); }
    }
    private static class TriggerElectionCommand extends RaftMessage {
        public TriggerElectionCommand() { super("INTERNAL", -1); }
    }

    private void sendToPeer(String peerId, RaftMessage message) {
        // Suppress heartbeat spam in logs
        if (!(message instanceof AppendEntriesRequest r && r.isHeartbeat()) &&
                !(message instanceof AppendEntriesResponse)) {
            System.out.println("   [Network] " + nodeId + " → " + peerId +
                    " : " + message.getClass().getSimpleName());
        }

        // FIX: restored ClusterSimulator routing for in-memory tests (ZenithChaosTest).
        // Docker deployment uses host:port peers — ClusterSimulator.activeNodes will be empty,
        // so it falls through to the TCP path automatically. Both modes work correctly.
        if (ClusterSimulator.activeNodes.containsKey(peerId)) {
            ClusterSimulator.routeMessage(peerId, message);
            return;
        }

        // TCP path — only for host:port formatted peers (Docker / AWS deployment)
        if (!peerId.contains(":")) {
            System.out.println("⚠️ Cannot route to peer: " + peerId + " (not in ClusterSimulator and no host:port)");
            return;
        }

        rpcPool.submit(() -> {
            String[] parts = peerId.split(":");
            try (java.net.Socket socket = new java.net.Socket(parts[0], Integer.parseInt(parts[1]));
                 java.io.PrintWriter out = new java.io.PrintWriter(socket.getOutputStream(), true)) {
                socket.setSoTimeout(500);
                out.println(MessageSerializer.toJson(message));
            } catch (java.io.IOException ignored) {
                // peer unreachable — normal during elections
            }
        });
    }
    public void shutdown() {

        running = false;

        if (electionTimer != null)
            electionTimer.cancel(true);

        if (heartbeatTimer != null)
            heartbeatTimer.cancel(true);

        scheduler.shutdownNow();
        rpcPool.shutdownNow();

        inbox.clear();

        try {
            wal.close();
        } catch (Exception ignored) {}

        metrics.setLeader(false);

        System.out.println("💀 Node [" + nodeId + "] shut down.");
    }
}