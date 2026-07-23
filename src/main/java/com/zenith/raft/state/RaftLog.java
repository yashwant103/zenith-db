//package com.zenith.raft.state;
//
//import com.zenith.raft.rpc.AppendEntriesRequest.LogEntryDTO;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * RaftLog — The staging area for distributed trades.
// * Entries go here FIRST. They only move to the MemoryEngine once a majority is reached.
// */
//public class RaftLog {
//
//    // The actual list of commands. (Index 0 in list = Log Index 0)
//    private final List<LogEntryDTO> entries = new ArrayList<>();
//
//    // Index of the highest log entry known to be committed (majority reached)
//    private volatile int commitIndex = -1;
//
//    // Index of the highest log entry applied to our local MemoryEngine
//    private volatile int lastApplied = -1;
//
//    public synchronized void appendEntry(LogEntryDTO entry) {
//        entries.add(entry);
//    }
//
//    // Used by Followers to safely overwrite conflicting logs if the Leader says so
//    public synchronized void appendEntriesFrom(int prevLogIndex, List<LogEntryDTO> newEntries) {
//        // Remove any conflicting entries that come after the prevLogIndex
//        while (entries.size() > prevLogIndex + 1) {
//            entries.remove(entries.size() - 1);
//        }
//        entries.addAll(newEntries);
//    }
//
//    public synchronized LogEntryDTO getEntry(int index) {
//        if (index < 0 || index >= entries.size()) return null;
//        return entries.get(index);
//    }
//
//    // ── Helper Methods for Raft RPCs ──
//
//    public synchronized int getLastLogIndex() {
//        return entries.size() - 1;
//    }
//
//    public synchronized int getLastLogTerm() {
//        int lastIndex = getLastLogIndex();
//        if (lastIndex == -1) return 0; // Log is empty
//        return entries.get(lastIndex).getTerm();
//    }
//
//    // Used by the Leader to grab all entries starting from a specific follower's nextIndex
//    public synchronized List<LogEntryDTO> getEntriesFrom(int nextIndex) {
//        if (nextIndex < 0 || nextIndex > entries.size()) {
//            return new ArrayList<>();
//        }
//        return new ArrayList<>(entries.subList(nextIndex, entries.size()));
//    }
//
//    // ── Commit Tracking ──
//
//    public int getCommitIndex() { return commitIndex; }
//
//    public void setCommitIndex(int commitIndex) {
//        this.commitIndex = commitIndex;
//    }
//
//    public int getLastApplied() { return lastApplied; }
//
//    public void setLastApplied(int lastApplied) {
//        this.lastApplied = lastApplied;
//    }
//}

package com.zenith.raft.state;

import com.zenith.raft.rpc.AppendEntriesRequest.LogEntryDTO;

import java.util.ArrayList;
import java.util.List;

/**
 * RaftLog — The staging area for distributed trades.
 * UPGRADED: Now supports Log Compaction (Snapshots) using a baseIndex offset!
 */
public class RaftLog {

    // The actual list of commands.
    private final List<LogEntryDTO> entries = new ArrayList<>();

    // ── NEW: SNAPSHOT TRACKING ──
    // The absolute Raft index of the first item in our 'entries' list.
    // If we compact the first 100 items, baseIndex becomes 101.
    private volatile int baseIndex = 0;
    private volatile int baseTerm = 0;

    private volatile int commitIndex = -1;
    private volatile int lastApplied = -1;

    public synchronized void appendEntry(LogEntryDTO entry) {
        entries.add(entry);
    }

    public synchronized void appendEntriesFrom(int prevLogIndex, List<LogEntryDTO> newEntries) {
        // Calculate where in our actual ArrayList this absolute Raft index falls
        int arrayIndex = prevLogIndex - baseIndex + 1;

        if (arrayIndex < 0) {
            // This means the Leader is trying to overwrite data we have already snapshotted and permanently saved!
            // In a full implementation, we'd handle this via InstallSnapshot RPC, but for now, we ignore it.
            return;
        }

        while (entries.size() > arrayIndex) {
            entries.remove(entries.size() - 1);
        }
        entries.addAll(newEntries);
    }

    public synchronized LogEntryDTO getEntry(int absoluteIndex) {
        int arrayIndex = absoluteIndex - baseIndex;
        if (arrayIndex < 0 || arrayIndex >= entries.size()) return null;
        return entries.get(arrayIndex);
    }

    public synchronized int getLastLogIndex() {
        return baseIndex + entries.size() - 1;
    }

    public synchronized int getLastLogTerm() {
        int lastIndex = getLastLogIndex();
        if (lastIndex < baseIndex) return baseTerm; // The log is completely empty/compacted
        return getEntry(lastIndex).getTerm();
    }

//    public synchronized List<LogEntryDTO> getEntriesFrom(int absoluteNextIndex) {
//        int arrayIndex = absoluteNextIndex - baseIndex;
//
//        if (arrayIndex < 0) {
//            // The follower is so far behind, the data it needs has already been compacted into a snapshot!
//            System.out.println("⚠️ Follower is too far behind! Requires Snapshot transfer.");
//            return new ArrayList<>();
//        }
//
//        if (arrayIndex > entries.size()) {
//            return new ArrayList<>();
//        }
//        return new ArrayList<>(entries.subList(arrayIndex, entries.size()));
//    }

    // Used by the Leader to grab entries starting from a specific follower's nextIndex
    public synchronized List<LogEntryDTO> getEntriesFrom(int absoluteNextIndex) {
        int arrayIndex = absoluteNextIndex - baseIndex;

        if (arrayIndex < 0) {
            System.out.println("⚠️ Follower is too far behind! Requires Snapshot transfer.");
            return new ArrayList<>();
        }
        if (arrayIndex >= entries.size()) {
            return new ArrayList<>();
        }

        // FIX: Pagination / Batching!
        // Never send more than 50 entries in a single network RPC to prevent OOM and JSON truncation.
        int toIndex = Math.min(entries.size(), arrayIndex + 50);
        return new ArrayList<>(entries.subList(arrayIndex, toIndex));
    }

    // ── NEW: THE COMPACTION TRIGGER ──
    // This deletes old, already-committed entries from RAM!
    public synchronized void compactLog(int upToAbsoluteIndex) {
        int arrayIndex = upToAbsoluteIndex - baseIndex;

        // We can only compact data that actually exists in our list and has been committed!
        if (arrayIndex >= 0 && arrayIndex < entries.size() && upToAbsoluteIndex <= commitIndex) {
            LogEntryDTO lastCompactedEntry = entries.get(arrayIndex);
            baseTerm = lastCompactedEntry.getTerm();

            // Delete everything from index 0 up to (and including) the arrayIndex
            entries.subList(0, arrayIndex + 1).clear();

            // Shift our offset forward
            baseIndex = upToAbsoluteIndex + 1;

            System.out.println("🧹 [RAFT LOG] Compacted! Deleted up to index " + upToAbsoluteIndex + ". New baseIndex is " + baseIndex);
        }
    }

    // ── Commit Tracking ──
    public int getCommitIndex() { return commitIndex; }
    public void setCommitIndex(int commitIndex) { this.commitIndex = commitIndex; }
    public int getLastApplied() { return lastApplied; }
    public void setLastApplied(int lastApplied) { this.lastApplied = lastApplied; }
    public int getBaseIndex() { return baseIndex; }
}