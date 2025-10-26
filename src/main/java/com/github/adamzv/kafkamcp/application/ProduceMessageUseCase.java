package com.github.adamzv.kafkamcp.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.ports.KafkaProducerPort;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class ProduceMessageUseCase {

  public static final String FORMAT_RAW = "raw-string";
  public static final String FORMAT_JSON = "json";

  private final KafkaProducerPort producerPort;
  private final ObjectMapper objectMapper;
  private final Limits limits;

  public ProduceMessageUseCase(KafkaProducerPort producerPort, ObjectMapper objectMapper, Limits limits) {
    this.producerPort = producerPort;
    this.objectMapper = objectMapper;
    this.limits = limits;
  }

  public ProduceResult execute(ProduceRequest request) {
    if (request == null) {
      throw Problems.invalidArgument("Produce request is required", Map.of());
    }

    String topic = request.topic();
    if (topic == null || topic.isBlank()) {
      throw Problems.invalidArgument("Topic must not be blank", Map.of());
    }

    String value = request.value();
    if (value == null) {
      throw Problems.invalidArgument("Message value must not be null", Map.of("topic", topic));
    }

    int payloadSize = value.getBytes(StandardCharsets.UTF_8).length;
    if (payloadSize > limits.messageBytes()) {
      throw Problems.payloadTooLarge(
          "Message exceeds configured size limit",
          Map.of("limitBytes", limits.messageBytes(), "payloadBytes", payloadSize)
      );
    }

    String format = normalizeFormat(request.format());
    if (FORMAT_JSON.equals(format)) {
      validateJson(value);
    }

    Map<String, String> headers = sanitizeHeaders(request.headers());
    ProduceRequest sanitized = new ProduceRequest(topic, format, request.key(), headers, value);
    return producerPort.produce(sanitized);
  }

  private String normalizeFormat(String format) {
    if (format == null || format.isBlank()) {
      return FORMAT_RAW;
    }
    String normalized = format.trim().toLowerCase();
    if (FORMAT_RAW.equals(normalized) || FORMAT_JSON.equals(normalized)) {
      return normalized;
    }
    throw Problems.invalidArgument("Unsupported format", Map.of("format", format));
  }

  private void validateJson(String value) {
    try {
      objectMapper.readTree(value);
    } catch (JsonProcessingException ex) {
      throw Problems.invalidArgument(
          "Value is not valid JSON",
          Map.of("error", ex.getOriginalMessage())
      );
    }
  }

  private Map<String, String> sanitizeHeaders(Map<String, String> headers) {
    if (headers == null || headers.isEmpty()) {
      return Map.of();
    }
    Map<String, String> copy = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      String key = entry.getKey();
      if (key == null || key.isBlank()) {
        throw Problems.invalidArgument("Header names must not be blank", Map.of());
      }
      copy.put(key, entry.getValue());
    }
    return java.util.Collections.unmodifiableMap(copy);
  }
}
