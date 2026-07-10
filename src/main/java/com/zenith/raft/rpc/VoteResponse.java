package com.zenith.raft.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * VoteResponse — sent back to CANDIDATE after processing VoteRequest.
 *
 * FIX FROM YOUR VERSION:
 * Field changed from 'public final' to 'private' with getter.
 */
public class VoteResponse extends RaftMessage {

    private boolean voteGranted;

    public VoteResponse() {}

    @JsonCreator
    public VoteResponse(
            @JsonProperty("senderId")    String senderId,
            @JsonProperty("term")        int term,
            @JsonProperty("voteGranted") boolean voteGranted) {
        super(senderId, term);
        this.voteGranted = voteGranted;
    }

    public boolean isVoteGranted()           { return voteGranted; }
    public void    setVoteGranted(boolean v) { this.voteGranted = v; }

    @Override
    public String toString() {
        return String.format("VoteResponse{from='%s', term=%d, granted=%b}",
                getSenderId(), getTerm(), voteGranted);
    }
}