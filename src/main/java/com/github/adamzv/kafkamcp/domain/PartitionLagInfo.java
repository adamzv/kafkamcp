package com.github.adamzv.kafkamcp.domain;

public record PartitionLagInfo(
    String topic,
    int partition,
    Long currentOffset,
    Long endOffset,
    Long lag
) {
}
