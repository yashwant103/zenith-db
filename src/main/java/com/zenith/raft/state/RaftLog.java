package com.zenith.raft.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zenith.raft.rpc.AppendEntriesRequest.LogEntryDTO;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * RaftLog — The staging area for distributed trades.
 * UPGRADED: Now supports Log Compaction (Snapshots) using a baseIndex offset!
 *
 * PERSISTENCE:
 * Previously this whole class was in-memory only. That's a real gap in a
 * Raft implementation: the paper requires the log to be durable before a
 * node responds to an AppendEntries or grants a vote, because the log
 * (even its uncommitted tail) is what the "candidate's log is at least as
 * up-to-date as mine" freshness check in RaftNode.handleVoteRequest relies
 * on. Without this, a node that restarts looks artificially "empty" to
 * that check and can end up voting for a candidate that's actually behind
 * the cluster.
 *
 * DESIGN CHOICE — whole-state rewrite instead of an incremental log file:
 * A "real" production Raft log would be its own append-only file with
 * periodic compaction, mirroring WriteAheadLog. Given SNAPSHOT_THRESHOLD
 * (RaftNode) is 5, this log never holds more than a handful of entries
 * before it's compacted anyway — so on every structural change (append,
 * replicate, compact) this simply re-serializes the whole small entries
 * list to disk atomically (temp file + atomic rename, same pattern as
 * RaftState). Simple, correct, and cheap at this scale. If SNAPSHOT_THRESHOLD
 * were raised by orders of magnitude for a higher-throughput deployment,
 * this would need to become a real incremental append-only format instead.
 */
public class RaftLog {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // The actual list of commands.
    private final List<LogEntryDTO> entries = new ArrayList<>();

    // ── NEW: SNAPSHOT TRACKING ──
    // The absolute Raft index of the first item in our 'entries' list.
    // If we compact the first 100 items, baseIndex becomes 101.
    private volatile int baseIndex = 0;
    private volatile int baseTerm = 0;

    private volatile int commitIndex = -1;
    private volatile int lastApplied = -1;

    // ── Persistence ──
    private volatile Path persistPath = null;

    /** Container for the on-disk representation. Public fields — Jackson
     *  auto-detects public fields by default, no extra annotations needed. */
    private static class PersistedLog {
        public int baseIndex;
        public int baseTerm;
        public int commitIndex;
        public int lastApplied;
        public List<LogEntryDTO> entries = new ArrayList<>();
    }

    public synchronized void enablePersistence(Path path) throws IOException {
        this.persistPath = path;
        if (Files.exists(path)) {
            PersistedLog loaded = MAPPER.readValue(path.toFile(), PersistedLog.class);
            baseIndex   = loaded.baseIndex;
            baseTerm    = loaded.baseTerm;
            commitIndex = loaded.commitIndex;
            lastApplied = loaded.lastApplied;
            entries.clear();
            entries.addAll(loaded.entries);
            System.out.println("♻️ Restored persisted Raft log: " + loaded.entries.size() +
                    " entries, baseIndex=" + loaded.baseIndex + ", lastApplied=" + loaded.lastApplied);
        } else {
            persistToDisk(); // create the file with initial empty state
        }
    }

    // NOTE: everything in this class synchronizes on 'this' (the object
    // monitor), same as every pre-existing method here — deliberately a
    // single lock, not a second lock object, to avoid any risk of a
    // lock-order-inversion deadlock between "mutate entries then persist"
    // and "load persisted state into entries" call paths.
    private synchronized void persistToDisk() {
        Path path = persistPath;
        if (path == null) return; // persistence not enabled — fine for tests that don't need it

        PersistedLog snapshot = new PersistedLog();
        snapshot.baseIndex   = baseIndex;
        snapshot.baseTerm    = baseTerm;
        snapshot.commitIndex = commitIndex;
        snapshot.lastApplied = lastApplied;
        snapshot.entries     = new ArrayList<>(entries);

        try {
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            MAPPER.writeValue(tmp.toFile(), snapshot);
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("⚠️ CRITICAL: failed to persist Raft log: " + e.getMessage());
        }
    }

    /** Called by RaftNode once per apply-batch (not per entry) after a
     *  synchronous WAL flush, so "log says applied" never gets ahead of
     *  "trade data is actually durable on disk". */
    public void persistLastApplied() {
        persistToDisk();
    }

    public synchronized void appendEntry(LogEntryDTO entry) {
        entries.add(entry);
        persistToDisk();
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
        persistToDisk();
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

            persistToDisk();

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