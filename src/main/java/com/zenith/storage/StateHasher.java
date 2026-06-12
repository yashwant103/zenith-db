package com.zenith.storage;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.TreeMap;

public class StateHasher {
    public static String generateStateHash(MemoryEngine engine) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            // 1. Sort trades by ID so the hash is consistent across all nodes
            TreeMap<String, Trade> sortedTrades = new TreeMap<>(engine.getAllTrades());

            // 2. Build the state string
            StringBuilder sb = new StringBuilder();
            for (Trade t : sortedTrades.values()) {
                sb.append(t.tradeId()).append(t.tickerSymbol())
                        .append(t.amount()).append(t.price()).append(t.status());
            }

            // 3. Generate SHA-256 fingerprint
            byte[] hash = digest.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 failed", e);
        }
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}