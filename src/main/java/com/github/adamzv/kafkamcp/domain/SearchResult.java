package com.github.adamzv.kafkamcp.domain;

import java.util.List;

public record SearchResult(
    List<MessageEnvelope> messages,
    int messagesScanned,
    boolean limitReached,
    boolean maxScanReached,
    long searchDurationMs
) {}
