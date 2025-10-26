package com.github.adamzv.kafkamcp.domain;

public record TailRequest(
    String topic,
    String from,
    Integer limit,
    Integer partition
) {}
