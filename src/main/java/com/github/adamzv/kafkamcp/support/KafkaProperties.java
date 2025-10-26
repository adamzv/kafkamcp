package com.github.adamzv.kafkamcp.support;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "kafka")
public record KafkaProperties(
    @NotBlank(message = "kafka.bootstrapServers must not be blank")
    String bootstrapServers
) {}
