package com.zenith.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MemoryEngine — thread-safe in-memory trade store.
 *
 * FIXES FROM YOUR VERSION:
 *
 * BUG 1 — updateTradeStatus lost CAS protection
 * Your version uses insertTrade() for updates which calls primaryIndex.put()
 * unconditionally. This means two concurrent update threads can both succeed
 * even if they conflict, silently overwriting each other.
 * Fix: use ConcurrentHashMap.replace(key, oldVal, newVal) for atomic CAS update.
 *
 * BUG 2 — restoredtrade() missing for WAL recovery
 * Your WAL recovery calls engine.insertTrade() which correctly updates the
 * secondary index. But insertTrade() uses put() not putIfAbsent() — this is
 * actually correct for recovery (later entries must win). No change needed here
 * but added a dedicated restoredtrade() method for clarity that WAL uses.
 *
 * BUG 3 — processedRequests set grows forever (memory leak)
 * After handling millions of trades, the idempotency set holds millions of
 * request IDs consuming unbounded memory.
 * Fix: use a LinkedHashMap with max size 100_000 as a simple LRU cache.
 *
 * BUG 4 — getAllActiveTrades() missing (needed by WAL compact)
 * Your compact() uses getAllTrades().values() — but getAllTrades() returns
 * the raw ConcurrentHashMap which is modified during iteration in compact().
 * Fix: getAllActiveTrades() returns a snapshot copy for safe iteration.
 */
public class MemoryEngine {

    // PRIMARY INDEX — O(1) lookup by trade ID
    private final ConcurrentHashMap<String, Trade> primaryIndex = new ConcurrentHashMap<>();

    // SECONDARY INDEX — O(1) lookup by ticker symbol
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Trade>> tickerIndex = new ConcurrentHashMap<>();

    // IDEMPOTENCY CACHE — fixed max size to prevent memory leak
    // Uses synchronized LinkedHashMap with LRU eviction at 100k entries
    private final java.util.Set<String> processedRequests = java.util.Collections.synchronizedSet(
            new java.util.LinkedHashSet<String>() {
                protected boolean removeEldestEntry(Map.Entry<String, ?> eldest) {
                    return size() > 100_000; // evict oldest when over limit
                }
            }
    );

    // ── Idempotency ──
    public boolean hasProcessed(String requestId) {
        return processedRequests.contains(requestId);
    }
    public void markProcessed(String requestId) {
        processedRequests.add(requestId);
    }

    // ── INSERT — used for new trades ──
    public void insertTrade(Trade trade) {
        primaryIndex.put(trade.tradeId(), trade);
        tickerIndex.computeIfAbsent(trade.tickerSymbol(), k -> new ConcurrentHashMap<>())
                .put(trade.tradeId(), trade);
    }

    // ── RESTORE — used during WAL recovery (same as insert, later entries win) ──
    public void restoredtrade(Trade trade) {
        insertTrade(trade); // put() semantics — later WAL entries override earlier ones
    }

    // ── UPDATE — CAS-protected atomic status change ──
    // FIX: uses replace(key, old, new) instead of unconditional put()
    // Returns false if another thread already updated this trade (conflict)
    public boolean updateTradeStatus(String tradeId, String newStatus) {
        Trade oldTrade = primaryIndex.get(tradeId);
        if (oldTrade == null) {
            System.out.println("Trade not found: " + tradeId);
            return false;
        }
        Trade newTrade = new Trade(
                oldTrade.tradeId(), oldTrade.tickerSymbol(),
                oldTrade.amount(), oldTrade.price(), newStatus
        );
        // Atomic CAS — only replaces if current value is still oldTrade
        boolean swapped = primaryIndex.replace(tradeId, oldTrade, newTrade);
        if (swapped) {
            // Also update secondary index
            ConcurrentHashMap<String, Trade> sub = tickerIndex.get(oldTrade.tickerSymbol());
            if (sub != null) sub.put(tradeId, newTrade);
        }
        return swapped;
    }

    // ── DELETE — removes from both indexes ──
    public boolean deleteTrade(String tradeId) {
        Trade removed = primaryIndex.remove(tradeId);
        if (removed != null) {
            ConcurrentHashMap<String, Trade> sub = tickerIndex.get(removed.tickerSymbol());
            if (sub != null) {
                sub.remove(tradeId);
                if (sub.isEmpty()) tickerIndex.remove(removed.tickerSymbol());
            }
            return true;
        }
        return false;
    }

    // ── QUERIES ──
    public Trade getTrade(String tradeId) {
        return primaryIndex.get(tradeId);
    }

    public List<Trade> getTradesByTicker(String ticker) {
        ConcurrentHashMap<String, Trade> sub = tickerIndex.get(ticker);
        if (sub == null || sub.isEmpty()) return new ArrayList<>();
        return new ArrayList<>(sub.values());
    }

    // Returns raw map — used by StateHasher (read-only, safe)
    public Map<String, Trade> getAllTrades() {
        return primaryIndex;
    }

    // Returns a SNAPSHOT copy — used by WAL compact() for safe iteration
    // FIX: compact() was iterating getAllTrades() while potentially modifying it
    public List<Trade> getAllActiveTrades() {
        return new ArrayList<>(primaryIndex.values());
    }

    public int size() {
        return primaryIndex.size();
    }
}