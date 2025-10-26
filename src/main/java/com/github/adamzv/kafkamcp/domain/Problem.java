package com.github.adamzv.kafkamcp.domain;

import java.util.Map;

public record Problem(
    String code,
    String message,
    Map<String, Object> details
) {}
