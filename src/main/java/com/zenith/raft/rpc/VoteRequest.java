package com.zenith.raft.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * VoteRequest — sent by CANDIDATE to all peers during leader election.
 *
 * FIX FROM YOUR VERSION:
 * Fields changed from 'public final' to 'private' with getters.
 * @JsonCreator constructor is correct — keep it exactly as is.
 * The name "VOTE_REQUEST" in @JsonSubTypes must match — updated in RaftMessage.
 */
public class VoteRequest extends RaftMessage {

    private int lastLogIndex;
    private int lastLogTerm;

    // Required by Jackson for deserialization
    public VoteRequest() {}

    @JsonCreator
    public VoteRequest(
            @JsonProperty("senderId")     String senderId,
            @JsonProperty("term")         int term,
            @JsonProperty("lastLogIndex") int lastLogIndex,
            @JsonProperty("lastLogTerm")  int lastLogTerm) {
        super(senderId, term);
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm  = lastLogTerm;
    }

    public int  getLastLogIndex()      { return lastLogIndex; }
    public int  getLastLogTerm()       { return lastLogTerm; }
    public void setLastLogIndex(int i) { this.lastLogIndex = i; }
    public void setLastLogTerm(int t)  { this.lastLogTerm = t; }

    @Override
    public String toString() {
        return String.format("VoteRequest{from='%s', term=%d, lastLogIndex=%d, lastLogTerm=%d}",
                getSenderId(), getTerm(), lastLogIndex, lastLogTerm);
    }
}