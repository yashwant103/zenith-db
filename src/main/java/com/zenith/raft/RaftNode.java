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

    // Leader-lease tracking for linearizable reads — see isSafeForLinearizableRead().
    private final Map<String, Long> lastAckTimeMillis = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    public RaftNode(String nodeId, List<String> peers, RaftState state,
                    MemoryEngine engine, WriteAheadLog wal, ZenithMetrics metrics) {
        this.nodeId  = nodeId;
        this.peers   = peers;
        this.state   = state;
        this.engine  = engine;
        this.wal     = wal;
        this.metrics = metrics;
        this.wal.setMetrics(metrics); // FIX: without this, zenith_wal_flushes_total never moves

        // FIX: persist currentTerm/votedFor so a restart can't cause double-voting
        // in a term this node already voted in (a real split-brain risk otherwise).
        // Also persist the log itself — see RaftLog's class doc for why an
        // in-memory-only log is a real gap, not just term/vote.
        // Co-located next to this node's WAL file so it inherits the same
        // per-node/per-test/per-simulator-instance isolation the WAL path already has.
        try {
            String safeId = nodeId.replaceAll("[^a-zA-Z0-9_.-]", "_");
            java.nio.file.Path raftStateFile = wal.getWalPath().resolveSibling(safeId + ".raftstate");
            java.nio.file.Path raftLogFile   = wal.getWalPath().resolveSibling(safeId + ".raftlog");
            this.state.enablePersistence(raftStateFile);
            this.state.getLog().enablePersistence(raftLogFile);
        } catch (Exception e) {
            System.err.println("⚠️ Could not enable Raft state/log persistence for " + nodeId + ": " + e.getMessage());
        }
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

        grantedVotes.clear();
        grantedVotes.add(nodeId); // candidate always votes for itself
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

        // Any response that gets here (success OR a log-mismatch rejection)
        // already passed the term check above, which means this peer still
        // recognized us as the legitimate leader for the current term at the
        // moment it replied. That's exactly the "proof of life" the leader
        // lease in isSafeForLinearizableRead() needs — record it regardless
        // of which branch below runs.
        lastAckTimeMillis.put(resp.getSenderId(), System.currentTimeMillis());

        if (resp.isSuccess()) {
            matchIndex.put(resp.getSenderId(), resp.getMatchIndex());
            nextIndex.put(resp.getSenderId(),  resp.getMatchIndex() + 1);
            advanceCommitIndex();
        } else {
            // FIX: AppendEntriesResponse.rejected() already tells us the follower's
            // real last index via getMatchIndex() — jump nextIndex straight there
            // instead of decrementing by 1 per heartbeat. The old code ignored this
            // hint entirely, so a follower that was N entries behind took N heartbeat
            // round trips (at 200ms each) to catch up instead of one.
            // matchIndex is the follower's actual last log index (-1 if its log is
            // empty), so matchIndex + 1 is always the correct next index to try.
            int base = state.getLog().getBaseIndex();
            nextIndex.put(resp.getSenderId(), Math.max(base, resp.getMatchIndex() + 1));
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
        boolean appliedAny = false;
        while (state.getLog().getLastApplied() < state.getLog().getCommitIndex()) {
            int idx = state.getLog().getLastApplied() + 1;
            AppendEntriesRequest.LogEntryDTO entry = state.getLog().getEntry(idx);
            if (entry != null && !"INTERNAL_HB".equals(entry.getOperation())) {
                executeOnStateMachine(entry);
                state.getLog().setLastApplied(idx);
                appliedAny = true;
            } else break;
        }

        if (appliedAny) {
            // FIX: force the WAL to durably fsync *before* the raft log persists
            // the new lastApplied value. Without this ordering, a crash right
            // after persisting lastApplied (but before the WAL's own periodic
            // flush caught up) could leave the raft log claiming an entry was
            // applied when its actual trade data hadn't hit disk yet.
            wal.flushNow();
            state.getLog().persistLastApplied();
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
                wal.appendTrade(t, requestId);
                engine.insertTrade(t);
                metrics.recordTrade();
                metrics.activeTrades.set(engine.size());
            } else if ("UPDATE".equals(parts[0])) {
                if (engine.updateTradeStatus(parts[1], parts[2])) {
                    wal.appendTrade(engine.getTrade(parts[1]), requestId);
                }
            } else if ("DELETE".equals(parts[0])) {
                if (engine.deleteTrade(parts[1])) {
                    wal.appendTrade(new Trade(parts[1], "DELETED", 0, 0.0, "DELETED"), requestId);
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

    // Read-only accessors — needed for tests to assert on cluster-wide
    // invariants (e.g. "at most one leader per term") without reaching into
    // private state via reflection.
    public int getCurrentTerm() { return state.getCurrentTerm(); }
    public String getNodeId() { return nodeId; }
    public int getCommitIndex() { return state.getLog().getCommitIndex(); }

    // ── Linearizable read safety (leader lease) ──
    //
    // Previously any node — including a follower serving from its own
    // possibly-stale local state — answered SELECT directly. That was fixed
    // by redirecting reads to the leader, but "I am currently the leader" is
    // not by itself a linearizability guarantee: a leader that has just been
    // partitioned away from the majority doesn't find out until its own
    // heartbeats stop being acked, which can take up to one election timeout.
    // In that window it would otherwise keep answering reads with data that
    // may already be stale relative to a newly-elected leader on the other
    // side of the partition.
    //
    // DESIGN CHOICE — leader lease vs. a full ReadIndex round-trip:
    // The textbook fix is ReadIndex: on every read, the leader sends a fresh
    // round of heartbeats and blocks until a majority acks, *then* serves the
    // read. That's the more rigorous option, but doing it synchronously here
    // would block ZenithServer's single NIO selector thread for a heartbeat
    // round trip (tens to hundreds of ms) on every single read — while that
    // thread is also the one accepting new peer connections and reading
    // incoming Raft RPCs, so it would stall replication traffic too. Making
    // that safe would mean moving client command handling off the selector
    // thread entirely, which is a bigger architectural change than this pass
    // is scoped for.
    //
    // Instead: track the last time each peer acknowledged us (see
    // handleAppendEntriesResponse) and only serve a read if we've heard from
    // a majority within LEASE_WINDOW_MS. This is the same idea real systems
    // call a "leader lease" — cheap (a few volatile map reads, no blocking,
    // no new RPCs), but it relies on an explicit, stated assumption: clock
    // drift and scheduling delays between nodes stay well under the safety
    // margin below. That assumption is not new to this codebase — the
    // election timeout mechanism already depends on bounded relative timing
    // between nodes — but it is a real assumption, not a mathematical
    // guarantee the way a ReadIndex round-trip would be. Worth upgrading to
    // ReadIndex later if this needs to be airtight under adversarial clocks.
    //
    // LEASE_WINDOW_MS = 1500ms: heartbeats fire every 200ms (7.5x margin
    // before a single missed heartbeat could trip this), while staying
    // comfortably under the 4000ms minimum election timeout — so if we've
    // heard from a majority within the window, no new leader could have
    // completed an election yet even in the worst case.
    private static final long LEASE_WINDOW_MS = 1500;

    public boolean isSafeForLinearizableRead() {
        if (state.getRole() != RaftRole.LEADER) return false;

        long now = System.currentTimeMillis();
        int aliveWithinLease = 1; // count self
        for (String peer : peers) {
            Long last = lastAckTimeMillis.get(peer);
            if (last != null && (now - last) <= LEASE_WINDOW_MS) {
                aliveWithinLease++;
            }
        }
        int majority = (peers.size() + 1) / 2 + 1;
        if (aliveWithinLease < majority) return false;

        // Belt-and-suspenders: also make sure our own state machine isn't
        // momentarily behind our own commitIndex. In practice this window is
        // microseconds (applyCommittedEntries runs synchronously right after
        // advanceCommitIndex in the same call chain), but this read is called
        // from a different thread (ZenithServer's NIO loop) than that apply
        // loop, so it's worth a short bounded wait rather than assuming.
        long deadline = now + 50;
        while (state.getLog().getLastApplied() < state.getLog().getCommitIndex()) {
            if (System.currentTimeMillis() > deadline) return false;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

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
            try (java.net.Socket socket = new java.net.Socket(parts[0], Integer.parseInt(parts[1]))) {
                socket.setSoTimeout(500);
                byte[] payload = BinaryMessageCodec.encode(message);
                java.io.DataOutputStream out = new java.io.DataOutputStream(socket.getOutputStream());
                out.writeByte(BinaryMessageCodec.FRAME_MAGIC);
                out.writeInt(payload.length);
                out.write(payload);
                out.flush();
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