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
        System.out.println("⬇ Node downgraded to FOLLOWER for term " + newTerm);
    }

    public void convertToCandidate() {
        currentTerm.incrementAndGet(); // atomic increment — no race condition
        this.role     = RaftRole.CANDIDATE;
        this.votedFor = null;
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
    public void setVotedFor(String nodeId)        { this.votedFor = nodeId; }
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