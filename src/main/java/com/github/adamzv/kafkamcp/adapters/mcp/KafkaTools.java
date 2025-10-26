package com.github.adamzv.kafkamcp.adapters.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.adamzv.kafkamcp.application.DescribeTopicUseCase;
import com.github.adamzv.kafkamcp.application.ListConsumerGroupsUseCase;
import com.github.adamzv.kafkamcp.application.ListTopicsUseCase;
import com.github.adamzv.kafkamcp.application.ProduceMessageUseCase;
import com.github.adamzv.kafkamcp.application.TailTopicUseCase;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.Problem;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.domain.TailRequest;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.domain.TopicInfo;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.stereotype.Component;

@Component
public class KafkaTools {

  private static final Logger log = LoggerFactory.getLogger(KafkaTools.class);

  private final ListTopicsUseCase listTopicsUseCase;
  private final DescribeTopicUseCase describeTopicUseCase;
  private final ProduceMessageUseCase produceMessageUseCase;
  private final TailTopicUseCase tailTopicUseCase;
  private final ListConsumerGroupsUseCase listConsumerGroupsUseCase;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;

  public KafkaTools(
      ListTopicsUseCase listTopicsUseCase,
      DescribeTopicUseCase describeTopicUseCase,
      ProduceMessageUseCase produceMessageUseCase,
      TailTopicUseCase tailTopicUseCase,
      ListConsumerGroupsUseCase listConsumerGroupsUseCase,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry) {
    this.listTopicsUseCase = listTopicsUseCase;
    this.describeTopicUseCase = describeTopicUseCase;
    this.produceMessageUseCase = produceMessageUseCase;
    this.tailTopicUseCase = tailTopicUseCase;
    this.listConsumerGroupsUseCase = listConsumerGroupsUseCase;
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
  }

  @Tool(name = "listTopics", description = "List Kafka topics, optionally filtered by prefix")
  public List<TopicInfo> listTopics(String prefix) {
    return invoke(
        "listTopics",
        () -> listTopicsUseCase.execute(prefix),
        topics -> new ToolTelemetry(
            topics.size(),
            estimateTopicNameBytes(topics),
            contextOf("topicCount", topics.size())
        ),
        contextOf("prefix", prefix)
    );
  }

  @Tool(name = "describeTopic", description = "Describe a Kafka topic including partition metadata")
  public TopicDescriptionResult describeTopic(DescribeTopicInput input) {
    return invoke(
        "describeTopic",
        () -> describeTopicUseCase.execute(input.topic()),
        description -> new ToolTelemetry(
            description.partitions().size(),
            0L,
            contextOf(
                "topic", description.topic(),
                "partitionCount", description.partitions().size(),
                "internal", description.internal()
            )
        ),
        contextOf("topic", input.topic())
    );
  }

  @Tool(name = "produceMessage", description = "Produce a message to a Kafka topic")
  public ProduceResult produceMessage(ProduceRequest request) {
    long payloadBytes = estimatePayloadBytes(request.value());
    return invoke(
        "produceMessage",
        () -> produceMessageUseCase.execute(request),
        result -> new ToolTelemetry(
            1L,
            payloadBytes,
            contextOf(
                "topic", result.topic(),
                "partition", result.partition(),
                "offset", result.offset()
            )
        ),
        contextOf(
            "topic", request.topic(),
            "format", request.format(),
            "keyPresent", request.key() != null,
            "headerCount", request.headers() != null ? request.headers().size() : 0
        )
    );
  }

  @Tool(name = "tailTopic", description = "Tail recent messages from a Kafka topic")
  public List<MessageEnvelope> tailTopic(TailRequest request) {
    return invoke(
        "tailTopic",
        () -> tailTopicUseCase.execute(request),
        messages -> new ToolTelemetry(
            messages.size(),
            estimateMessageBytes(messages),
            contextOf(
                "topic", request.topic(),
                "partition", request.partition(),
                "from", request.from()
            )
        ),
        contextOf(
            "topic", request.topic(),
            "from", request.from(),
            "limit", request.limit(),
            "partition", request.partition()
        )
    );
  }

  @Tool(name = "listConsumerGroups", description = "List Kafka consumer groups")
  public List<Map<String, Object>> listConsumerGroups(String prefix) {
    return invoke(
        "listConsumerGroups",
        () -> listConsumerGroupsUseCase.execute(prefix),
        groups -> new ToolTelemetry(
            groups.size(),
            0L,
            contextOf("groupCount", groups.size())
        ),
        contextOf("prefix", prefix)
    );
  }

  private <T> T invoke(String tool,
                      Supplier<T> action,
                      Function<T, ToolTelemetry> summarizer,
                      Map<String, Object> requestContext) {
    String requestId = UUID.randomUUID().toString();
    Instant start = Instant.now();
    Timer.Sample sample = Timer.start(meterRegistry);
    try {
      T result = action.get();
      Duration duration = Duration.between(start, Instant.now());
      ToolTelemetry telemetry = summarizer.apply(result);
      recordSuccess(tool, telemetry, sample);
      log.info(
          "tool_call outcome=success requestId={} tool={} durationMs={} count={} bytesOut={} context={}",
          requestId,
          tool,
          duration.toMillis(),
          telemetry.messages(),
          telemetry.bytes(),
          mergeContexts(requestContext, telemetry.extraContext())
      );
      return result;
    } catch (ProblemException ex) {
      Duration duration = Duration.between(start, Instant.now());
      recordError(tool, ex.problem(), sample);
      log.warn(
          "tool_call outcome=error requestId={} tool={} durationMs={} code={} message={} context={}",
          requestId,
          tool,
          duration.toMillis(),
          ex.problem().code(),
          ex.problem().message(),
          requestContext
      );
      Problem problem = ex.problem();
      throw new ToolProblemException(problem, ex, objectMapper);
    }
  }

  private void recordSuccess(String tool, ToolTelemetry telemetry, Timer.Sample sample) {
    sample.stop(meterRegistry.timer("kafka_mcp_tool_duration_seconds", "tool", tool));
    meterRegistry.counter("kafka_mcp_messages_out_total", "tool", tool)
        .increment(telemetry.messages());
    meterRegistry.counter("kafka_mcp_bytes_out_total", "tool", tool)
        .increment(telemetry.bytes());
  }

  private void recordError(String tool, Problem problem, Timer.Sample sample) {
    sample.stop(meterRegistry.timer("kafka_mcp_tool_duration_seconds", "tool", tool));
    if (problem != null) {
      meterRegistry.counter(
              "kafka_mcp_tool_errors_total",
              "tool", tool,
              "code", problem.code())
          .increment();
    }
  }

  private Map<String, Object> mergeContexts(Map<String, Object> requestContext, Map<String, Object> resultContext) {
    Map<String, Object> merged = new HashMap<>();
    if (requestContext != null) {
      merged.putAll(requestContext);
    }
    if (resultContext != null) {
      merged.putAll(resultContext);
    }
    return merged;
  }

  private long estimateTopicNameBytes(List<TopicInfo> topics) {
    return topics.stream()
        .mapToLong(topic -> topic.name() == null ? 0 : topic.name().getBytes(StandardCharsets.UTF_8).length)
        .sum();
  }

  private long estimateMessageBytes(List<MessageEnvelope> messages) {
    return messages.stream()
        .mapToLong(message -> estimatePayloadBytes(message.valueString()))
        .sum();
  }

  private long estimatePayloadBytes(String value) {
    return value == null ? 0L : value.getBytes(StandardCharsets.UTF_8).length;
  }

  public record DescribeTopicInput(String topic) {}

  private record ToolTelemetry(long messages, long bytes, Map<String, Object> extraContext) {}

  private Map<String, Object> contextOf(Object... keyValues) {
    Map<String, Object> context = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      String key = String.valueOf(keyValues[i]);
      Object value = keyValues[i + 1];
      context.put(key, value);
    }
    return context;
  }
}
