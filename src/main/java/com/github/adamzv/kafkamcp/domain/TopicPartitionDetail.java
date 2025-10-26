package com.github.adamzv.kafkamcp.domain;

import java.util.List;

/**
 * Describes the broker assignments for a single Kafka partition.
 */
public record TopicPartitionDetail(
    int partition,
    Integer leader,
    List<Integer> isr,
    List<Integer> replicas
) {}
