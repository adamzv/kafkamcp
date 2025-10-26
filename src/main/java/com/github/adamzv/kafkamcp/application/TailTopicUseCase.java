package com.github.adamzv.kafkamcp.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.TailRequest;
import com.github.adamzv.kafkamcp.ports.KafkaConsumerPort;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class TailTopicUseCase {

  private static final Pattern END_PATTERN = Pattern.compile("^end-([0-9]+)$");
  private static final Pattern OFFSET_PATTERN = Pattern.compile("^offset:([0-9]+)$");
  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^timestamp:([0-9]+)$");

  private final KafkaConsumerPort consumerPort;
  private final ObjectMapper objectMapper;
  private final Limits limits;

  public TailTopicUseCase(KafkaConsumerPort consumerPort, ObjectMapper objectMapper, Limits limits) {
    this.consumerPort = consumerPort;
    this.objectMapper = objectMapper;
    this.limits = limits;
  }

  public List<MessageEnvelope> execute(TailRequest request) {
    if (request == null) {
      throw Problems.invalidArgument("Tail request is required", Map.of());
    }

    String topic = request.topic();
    if (topic == null || topic.isBlank()) {
      throw Problems.invalidArgument("Topic must not be blank", Map.of());
    }

    String normalizedFrom = normalizeFrom(request.from());
    int normalizedLimit = normalizeLimit(request.limit());
    Integer partition = normalizePartition(request.partition());

    TailRequest normalized = new TailRequest(topic, normalizedFrom, normalizedLimit, partition);
    List<MessageEnvelope> rawMessages = consumerPort.tail(normalized, limits);

    if (rawMessages.isEmpty()) {
      return List.of();
    }

    List<MessageEnvelope> enriched = new ArrayList<>(rawMessages.size());
    for (MessageEnvelope message : rawMessages) {
      enriched.add(enrichWithJson(message));
    }
    return List.copyOf(enriched);
  }

  private String normalizeFrom(String from) {
    String value = (from == null || from.isBlank()) ? "end-50" : from.trim();

    if (END_PATTERN.matcher(value).matches()) {
      return value;
    }
    if (matchesNonNegative(OFFSET_PATTERN, value) || matchesNonNegative(TIMESTAMP_PATTERN, value)) {
      return value;
    }

    throw Problems.invalidArgument(
        "Unsupported from position",
        Map.of("from", value)
    );
  }

  private boolean matchesNonNegative(Pattern pattern, String value) {
    Matcher matcher = pattern.matcher(value);
    if (!matcher.matches()) {
      return false;
    }
    long parsed = Long.parseLong(matcher.group(1));
    if (parsed < 0) {
      throw Problems.invalidArgument("Position must be non-negative", Map.of("from", value));
    }
    return true;
  }

  private int normalizeLimit(Integer limit) {
    int effective = limit == null ? limits.messagesPerCall() : limit;
    if (effective <= 0) {
      throw Problems.invalidArgument("Limit must be positive", Map.of("limit", effective));
    }
    return Math.min(effective, limits.messagesPerCall());
  }

  private Integer normalizePartition(Integer partition) {
    if (partition == null) {
      return null;
    }
    if (partition < 0) {
      throw Problems.invalidArgument("Partition must be non-negative", Map.of("partition", partition));
    }
    return partition;
  }

  private MessageEnvelope enrichWithJson(MessageEnvelope message) {
    String value = message.valueString();
    if (value == null || value.isBlank()) {
      return message;
    }
    try {
      JsonNode json = objectMapper.readTree(value);
      return new MessageEnvelope(
          message.key(),
          message.headers(),
          message.valueString(),
          json,
          message.timestamp(),
          message.partition(),
          message.offset()
      );
    } catch (JsonProcessingException ex) {
      return message;
    }
  }
}
