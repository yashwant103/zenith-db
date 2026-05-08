package com.zenith.raft.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * AppendEntriesResponse — follower's reply to leader after processing AppendEntriesRequest.
 *
 * ══════════════════════════════════════════════════════
 * WHEN success = true
 * ══════════════════════════════════════════════════════
 * Follower successfully appended all entries.
 * matchIndex = index of the last entry follower now has.
 * Leader updates its record of this follower's progress:
 *   nextIndex[followerId]  = matchIndex + 1
 *   matchIndex[followerId] = matchIndex
 * Leader checks: if matchIndex[followerId] >= N for a majority of followers
 *   → entry N is committed → apply to ZenithDB storage → reply to client.
 *
 * ══════════════════════════════════════════════════════
 * WHEN success = false
 * ══════════════════════════════════════════════════════
 * Two possible reasons:
 *
 * Reason 1 — Term mismatch:
 *   Leader's term < follower's currentTerm.
 *   The "leader" is actually stale — a new election already happened.
 *   Leader must step down and become FOLLOWER.
 *   Leader reads response.getTerm() and calls convertToFollower(response.getTerm()).
 *
 * Reason 2 — Log mismatch (prevLogIndex/prevLogTerm check failed):
 *   Follower doesn't have the entry at prevLogIndex, or has it with a different term.
 *   Follower is behind. Leader decrements nextIndex[followerId] and retries with
 *   an earlier prevLogIndex until follower accepts.
 *   matchIndex tells the leader where the follower's log actually ends.
 *
 * ══════════════════════════════════════════════════════
 * JSON EXAMPLES
 * ══════════════════════════════════════════════════════
 *
 * Success:
 * {
 *   "type": "APPEND_ENTRIES_RESP",
 *   "senderId": "node-2",
 *   "term": 3,
 *   "success": true,
 *   "matchIndex": 11
 * }
 *
 * Failure — log mismatch:
 * {
 *   "type": "APPEND_ENTRIES_RESP",
 *   "senderId": "node-2",
 *   "term": 3,
 *   "success": false,
 *   "matchIndex": 8
 * }
 *
 * Failure — stale leader:
 * {
 *   "type": "APPEND_ENTRIES_RESP",
 *   "senderId": "node-2",
 *   "term": 5,
 *   "success": false,
 *   "matchIndex": -1
 * }
 */
public class AppendEntriesResponse extends RaftMessage {

    private boolean success;

    /**
     * matchIndex — the index of the last log entry the follower now has.
     *
     * On success: matchIndex = index of last newly appended entry.
     *   Leader uses this to update its matchIndex[] map and check commit.
     *
     * On failure (log mismatch): matchIndex = follower's actual last index.
     *   Leader uses this to jump nextIndex[] to the right retry point
     *   instead of decrementing one by one (optimization).
     *
     * On failure (stale leader): matchIndex = -1 (not meaningful).
     */
    private int matchIndex;

    // Jackson no-args constructor
    public AppendEntriesResponse() {}

    @JsonCreator
    public AppendEntriesResponse(
            @JsonProperty("senderId")   String senderId,
            @JsonProperty("term")       int term,
            @JsonProperty("success")    boolean success,
            @JsonProperty("matchIndex") int matchIndex) {
        super(senderId, term);
        this.success    = success;
        this.matchIndex = matchIndex;
    }

    // ── Factory methods for clean construction ──

    // Heartbeat or replication accepted
    public static AppendEntriesResponse accepted(String nodeId, int term, int matchIndex) {
        return new AppendEntriesResponse(nodeId, term, true, matchIndex);
    }

    // Log mismatch — follower sends its actual last index so leader can jump there
    public static AppendEntriesResponse rejected(String nodeId, int term, int myLastIndex) {
        return new AppendEntriesResponse(nodeId, term, false, myLastIndex);
    }

    // Stale leader detected — follower sends its higher term so leader steps down
    public static AppendEntriesResponse staleLeader(String nodeId, int myTerm) {
        return new AppendEntriesResponse(nodeId, myTerm, false, -1);
    }

    // ── Getters ──
    public boolean isSuccess()    { return success; }
    public int     getMatchIndex(){ return matchIndex; }

    // ── Setters ──
    public void setSuccess(boolean s)    { this.success = s; }
    public void setMatchIndex(int m)     { this.matchIndex = m; }

    @Override
    public String toString() {
        return String.format("AppendEntriesResponse{from='%s', term=%d, success=%b, matchIndex=%d}",
                getSenderId(), getTerm(), success, matchIndex);
    }
}