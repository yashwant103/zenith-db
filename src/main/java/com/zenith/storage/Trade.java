package com.zenith.storage;

public record Trade(
    String tradeId,
    String tickerSymbol,
    int amount,
    double price,
    String status
) { }
