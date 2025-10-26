package com.github.adamzv.kafkamcp.domain;

public record ProduceResult(
    String topic,
    int partition,
    long offset,
    long timestamp
) {}
