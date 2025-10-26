package com.github.adamzv.kafkamcp.domain;

import java.util.Map;

public record ProduceRequest(
    String topic,
    String format,
    String key,
    Map<String, String> headers,
    String value
) {}
