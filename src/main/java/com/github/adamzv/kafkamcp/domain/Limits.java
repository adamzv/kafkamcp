package com.github.adamzv.kafkamcp.domain;

public record Limits(
    int messagesPerCall,
    int bytesPerCall,
    int messageBytes
) {}
