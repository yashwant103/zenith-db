package com.zenith.raft.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class AppendEntriesRequest extends RaftMessage {

    private int               prevLogIndex;
    private int               prevLogTerm;
    private List<LogEntryDTO> entries;
    private int               leaderCommit;

    public AppendEntriesRequest() {}

    @JsonCreator
    public AppendEntriesRequest(
            @JsonProperty("senderId")     String senderId,
            @JsonProperty("term")         int term,
            @JsonProperty("prevLogIndex") int prevLogIndex,
            @JsonProperty("prevLogTerm")  int prevLogTerm,
            @JsonProperty("entries")      List<LogEntryDTO> entries,
            @JsonProperty("leaderCommit") int leaderCommit) {
        super(senderId, term);
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm  = prevLogTerm;
        this.entries      = (entries != null) ? entries : Collections.emptyList();
        this.leaderCommit = leaderCommit;
    }

    public static AppendEntriesRequest heartbeat(
            String leaderId, int term,
            int prevLogIndex, int prevLogTerm,
            int leaderCommit) {
        return new AppendEntriesRequest(
                leaderId, term,
                prevLogIndex, prevLogTerm,
                Collections.emptyList(),
                leaderCommit
        );
    }

    // FIX: @JsonIgnore — stops Jackson from treating isHeartbeat() as a JSON field.
    //
    // WHY THIS HAPPENS:
    // Jackson scans all public methods. It sees isHeartbeat() → boolean return type
    // → follows JavaBean convention → treats it as property "heartbeat".
    // Serialization: adds "heartbeat":true to JSON output.
    // Deserialization: looks for "heartbeat" field in class → not found → crash.
    //
    // RULE TO REMEMBER:
    // Any method named isXxx() or getXxx() with no parameters gets auto-included
    // by Jackson unless you add @JsonIgnore. Always add @JsonIgnore to computed
    // convenience methods that are NOT actual stored fields.
    @JsonIgnore
    public boolean isHeartbeat() {
        return entries == null || entries.isEmpty();
    }

    // Getters
    public int               getPrevLogIndex() { return prevLogIndex; }
    public int               getPrevLogTerm()  { return prevLogTerm; }
    public List<LogEntryDTO> getEntries()      { return entries; }
    public int               getLeaderCommit() { return leaderCommit; }

    // Setters
    public void setPrevLogIndex(int i)          { this.prevLogIndex = i; }
    public void setPrevLogTerm(int t)           { this.prevLogTerm = t; }
    public void setEntries(List<LogEntryDTO> e) { this.entries = e; }
    public void setLeaderCommit(int c)          { this.leaderCommit = c; }

    @Override
    public String toString() {
        return String.format(
                "AppendEntries{from='%s', term=%d, prevIdx=%d, entries=%d, commit=%d, %s}",
                getSenderId(), getTerm(), prevLogIndex,
                entries.size(), leaderCommit,
                isHeartbeat() ? "HEARTBEAT" : "REPLICATION"
        );
    }

    // ── Nested DTO — one log entry carried inside AppendEntriesRequest ──
    public static class LogEntryDTO {

        private int    index;
        private int    term;
        private String key;
        private String value;
        private String operation;
        private long   sequenceId;

        public LogEntryDTO() {}

        @JsonCreator
        public LogEntryDTO(
                @JsonProperty("index")      int index,
                @JsonProperty("term")       int term,
                @JsonProperty("key")        String key,
                @JsonProperty("value")      String value,
                @JsonProperty("operation")  String operation,
                @JsonProperty("sequenceId") long sequenceId) {
            this.index      = index;
            this.term       = term;
            this.key        = key;
            this.value      = value;
            this.operation  = operation;
            this.sequenceId = sequenceId;
        }

        public static LogEntryDTO set(int idx, int term, String key, String val, long seqId) {
            return new LogEntryDTO(idx, term, key, val, "SET", seqId);
        }

        public static LogEntryDTO delete(int idx, int term, String key, long seqId) {
            return new LogEntryDTO(idx, term, key, null, "DELETE", seqId);
        }

        public int    getIndex()      { return index; }
        public int    getTerm()       { return term; }
        public String getKey()        { return key; }
        public String getValue()      { return value; }
        public String getOperation()  { return operation; }
        public long   getSequenceId() { return sequenceId; }

        public void setIndex(int i)        { this.index = i; }
        public void setTerm(int t)         { this.term = t; }
        public void setKey(String k)       { this.key = k; }
        public void setValue(String v)     { this.value = v; }
        public void setOperation(String o) { this.operation = o; }
        public void setSequenceId(long s)  { this.sequenceId = s; }

        @Override
        public String toString() {
            return String.format("LogEntry{idx=%d, term=%d, op=%s, key='%s', seqId=%d}",
                    index, term, operation, key, sequenceId);
        }
    }
}