package com.github.adamzv.kafkamcp.domain;

import java.util.List;

/**
 * Detailed topic description used by the describeTopic tool.
 */
public record TopicDescriptionResult(
    String topic,
    boolean internal,
    List<TopicPartitionDetail> partitions
) {}
