package com.github.adamzv.kafkamcp.domain;

public record TopicInfo(
    String name,
    int partitions,
    short replication,
    boolean internal
) {}
