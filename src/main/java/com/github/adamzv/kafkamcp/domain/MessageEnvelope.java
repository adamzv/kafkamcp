package com.github.adamzv.kafkamcp.domain;

import java.util.Map;

public record MessageEnvelope(
    String key,
    Map<String, String> headers,
    String valueString,
    Object valueJson,
    long timestamp,
    int partition,
    long offset
) {}
