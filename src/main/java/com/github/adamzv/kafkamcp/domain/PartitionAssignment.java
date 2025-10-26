package com.github.adamzv.kafkamcp.domain;

public record PartitionAssignment(
    String topic,
    int partition
) {
}
