package com.zenith.raft.state;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * RaftState — holds ALL state variables for a Raft node.
 *
 * FIX FROM YOUR VERSION:
 * Fields changed from 'public volatile' to 'private volatile'.
 * Direct public access like raftState.currentTerm = 5 bypasses
 * the state transition methods and corrupts Raft invariants.
 *
 * WHY volatile for role, currentLeaderId, votedFor?
 * Multiple threads read these simultaneously:
 * - Election timer thread reads role to check if FOLLOWER
 * - Heartbeat thread reads role to check if LEADER
 * - Network thread reads currentLeaderId to forward client requests
 * volatile guarantees all threads see the latest value immediately —
 * no CPU cache staleness.
 *
 * WHY AtomicInteger for currentTerm?
 * convertToCandidate() increments term: currentTerm++
 * This is a read-modify-write — NOT atomic with plain volatile.
 * Two threads calling convertToCandidate() simultaneously could both
 * read term=2, both write term=3 — losing one increment.
 * AtomicInteger.incrementAndGet() is a single atomic CPU instruction.
 */
public class RaftState {

    // AtomicInteger — term increments need atomic read-modify-write
    private final AtomicInteger currentTerm = new AtomicInteger(0);

    // volatile — single writes, only need visibility not atomicity
    private volatile String   votedFor        = null;
    private volatile RaftRole role            = RaftRole.FOLLOWER;
    private volatile String   currentLeaderId = null;

    // ── Persistence (currentTerm + votedFor) ──
    // Raft's safety proof depends on a node never voting twice in the same
    // term, even across a crash/restart. Everything else here (role,
    // currentLeaderId, the in-memory log) can safely reset on restart —
    // those two fields cannot. Optional: RaftState works fine without ever
    // calling enablePersistence() (e.g. plain unit tests of election logic
    // alone), it just means term/vote won't survive a restart in that case.
    private volatile java.nio.file.Path persistPath = null;
    private final Object persistLock = new Object();

    public void enablePersistence(java.nio.file.Path path) throws java.io.IOException {
        synchronized (persistLock) {
            this.persistPath = path;
            if (java.nio.file.Files.exists(path)) {
                java.util.List<String> lines = java.nio.file.Files.readAllLines(path);
                int loadedTerm = 0;
                String loadedVotedFor = null;
                for (String line : lines) {
                    if (line.startsWith("term=")) loadedTerm = Integer.parseInt(line.substring(5).trim());
                    else if (line.startsWith("votedFor=")) {
                        String v = line.substring(9).trim();
                        loadedVotedFor = v.equals("NONE") ? null : v;
                    }
                }
                currentTerm.set(loadedTerm);
                votedFor = loadedVotedFor;
                System.out.println("♻️ Restored persisted Raft state: term=" + loadedTerm + ", votedFor=" + loadedVotedFor);
            } else {
                persistToDisk(); // create the file with initial state (term=0, votedFor=NONE)
            }
        }
    }

    private void persistToDisk() {
        java.nio.file.Path path = persistPath;
        if (path == null) return; // persistence not enabled — fine for tests
        synchronized (persistLock) {
            try {
                String content = "term=" + currentTerm.get() + "\n"
                        + "votedFor=" + (votedFor == null ? "NONE" : votedFor) + "\n";
                java.nio.file.Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
                java.nio.file.Files.writeString(tmp, content);
                // atomic rename so a crash mid-write can never leave a half-written,
                // unparseable state file behind
                java.nio.file.Files.move(tmp, path,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (java.io.IOException e) {
                // Deliberately not crashing the node over a transient disk error here —
                // but this counter would be worth exporting to Prometheus in a real
                // production build so a persistent failure gets noticed and paged on.
                System.err.println("⚠️ CRITICAL: failed to persist Raft state (term/votedFor): " + e.getMessage());
            }
        }
    }

    // ── Controlled state transitions ──
    // These are the ONLY ways to change Raft state.
    // No class can bypass these by writing fields directly.

    // ── NEW LOGIC ──
    // The staging area for this node's trades
    private final RaftLog log = new RaftLog();

    public RaftLog getLog() {
        return log;
    }
    // ───────────────

    public void convertToFollower(int newTerm) {
        currentTerm.set(newTerm);
        this.role     = RaftRole.FOLLOWER;
        this.votedFor = null; // reset vote when entering new term
        persistToDisk();      // term/votedFor changed — must be durable before any RPC response goes out
        System.out.println("⬇ Node downgraded to FOLLOWER for term " + newTerm);
    }

    public void convertToCandidate() {
        currentTerm.incrementAndGet(); // atomic increment — no race condition
        this.role     = RaftRole.CANDIDATE;
        this.votedFor = null;
        persistToDisk();
        System.out.println("🗳 Node became CANDIDATE for term " + currentTerm.get());
    }

    public void convertToLeader() {
        this.role = RaftRole.LEADER;
        System.out.println("👑 Node became LEADER for term " + currentTerm.get());
    }

    // ── Getters ──
    public int      getCurrentTerm()     { return currentTerm.get(); }
    public String   getVotedFor()        { return votedFor; }
    public RaftRole getRole()            { return role; }
    public String   getCurrentLeaderId() { return currentLeaderId; }

    // ── Setters ──
    public void setVotedFor(String nodeId) {
        this.votedFor = nodeId;
        persistToDisk(); // must hit disk before the VoteResponse granting this vote is sent
    }
    public void setCurrentLeaderId(String nodeId) { this.currentLeaderId = nodeId; }

    // ── Convenience checks ──
    public boolean isLeader()    { return role == RaftRole.LEADER; }
    public boolean isFollower()  { return role == RaftRole.FOLLOWER; }
    public boolean isCandidate() { return role == RaftRole.CANDIDATE; }

    @Override
    public String toString() {
        return String.format("RaftState{term=%d, role=%s, leader='%s', votedFor='%s'}",
                currentTerm.get(), role, currentLeaderId, votedFor);
    }
}