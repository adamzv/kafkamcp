package com.github.adamzv.kafkamcp.domain;

import java.util.List;

public record SearchRequest(
    String topic,
    String searchTerm,
    List<SearchTarget> searchIn,
    String from,
    Integer limit,
    Integer maxScan,
    Boolean caseSensitive,
    Long startTimestamp,
    Long endTimestamp
) {}
