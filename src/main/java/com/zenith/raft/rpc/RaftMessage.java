package com.zenith.raft.rpc;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * RaftMessage — base class for ALL Raft node-to-node messages.
 * Updated to register AppendEntriesRequest and AppendEntriesResponse.
 *
 * All 4 message types now registered:
 * VOTE_REQUEST         → VoteRequest
 * VOTE_RESPONSE        → VoteResponse
 * APPEND_ENTRIES       → AppendEntriesRequest
 * APPEND_ENTRIES_RESP  → AppendEntriesResponse
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = VoteRequest.class,          name = "VOTE_REQUEST"),
        @JsonSubTypes.Type(value = VoteResponse.class,         name = "VOTE_RESPONSE"),
        @JsonSubTypes.Type(value = AppendEntriesRequest.class, name = "APPEND_ENTRIES"),
        @JsonSubTypes.Type(value = AppendEntriesResponse.class,name = "APPEND_ENTRIES_RESP"),
})
public abstract class RaftMessage {

    private String senderId;
    private int    term;

    protected RaftMessage() {}

    protected RaftMessage(String senderId, int term) {
        this.senderId = senderId;
        this.term     = term;
    }

    public String getSenderId() { return senderId; }
    public int    getTerm()     { return term; }
    public void   setSenderId(String s) { this.senderId = s; }
    public void   setTerm(int t)        { this.term = t; }
}