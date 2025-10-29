package com.github.adamzv.kafkamcp.adapters.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.adamzv.kafkamcp.application.DescribeConsumerGroupUseCase;
import com.github.adamzv.kafkamcp.application.DescribeTopicUseCase;
import com.github.adamzv.kafkamcp.application.ListConsumerGroupsUseCase;
import com.github.adamzv.kafkamcp.application.ListTopicsUseCase;
import com.github.adamzv.kafkamcp.application.ProduceMessageUseCase;
import com.github.adamzv.kafkamcp.application.SearchMessagesUseCase;
import com.github.adamzv.kafkamcp.application.TailTopicUseCase;
import com.github.adamzv.kafkamcp.domain.ConsumerGroupDetail;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.Problem;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.SearchRequest;
import com.github.adamzv.kafkamcp.domain.SearchResult;
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
  private final SearchMessagesUseCase searchMessagesUseCase;
  private final ListConsumerGroupsUseCase listConsumerGroupsUseCase;
  private final DescribeConsumerGroupUseCase describeConsumerGroupUseCase;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;

  public KafkaTools(
      ListTopicsUseCase listTopicsUseCase,
      DescribeTopicUseCase describeTopicUseCase,
      ProduceMessageUseCase produceMessageUseCase,
      TailTopicUseCase tailTopicUseCase,
      SearchMessagesUseCase searchMessagesUseCase,
      ListConsumerGroupsUseCase listConsumerGroupsUseCase,
      DescribeConsumerGroupUseCase describeConsumerGroupUseCase,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry) {
    this.listTopicsUseCase = listTopicsUseCase;
    this.describeTopicUseCase = describeTopicUseCase;
    this.produceMessageUseCase = produceMessageUseCase;
    this.tailTopicUseCase = tailTopicUseCase;
    this.searchMessagesUseCase = searchMessagesUseCase;
    this.listConsumerGroupsUseCase = listConsumerGroupsUseCase;
    this.describeConsumerGroupUseCase = describeConsumerGroupUseCase;
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
  }

  @Tool(name = "listTopics", description = """
      List Kafka topics with optional filtering by prefix or suffix (mutually exclusive).

      Parameters:
      - prefix: Filter topics starting with this value (e.g., "orders", "payment"). Use empty string or null for no prefix filter.
      - suffix: Filter topics ending with this value (e.g., "-dlt" for dead letter topics, "-retry" for retry topics). Use empty string or null for no suffix filter.

      Note: Empty strings ("") are treated as null. You cannot specify both prefix and suffix - they are mutually exclusive.

      Returns: List of TopicInfo objects containing:
      - name: Topic name
      - partitions: Number of partitions
      - replication: Replication factor
      - internal: Whether this is an internal Kafka topic

      Examples:
      - List all topics: {prefix: "", suffix: ""}
      - List topics starting with "orders": {prefix: "orders", suffix: ""}
      - List error topics (DLT): {prefix: "", suffix: "-dlt"}
      """)
  public List<TopicInfo> listTopics(ListTopicsInput input) {
    String prefix = input != null ? input.prefix() : null;
    String suffix = input != null ? input.suffix() : null;
    return invoke(
        "listTopics",
        () -> listTopicsUseCase.execute(prefix, suffix),
        topics -> new ToolTelemetry(
            topics.size(),
            estimateTopicNameBytes(topics),
            contextOf("topicCount", topics.size())
        ),
        contextOf("prefix", prefix, "suffix", suffix)
    );
  }

  @Tool(name = "describeTopic", description = "Describe a Kafka topic including partition metadata")
  public TopicDescriptionResult describeTopic(DescribeTopicInput input) {
    if (input == null) {
      throw Problems.invalidArgument("DescribeTopicInput is required", Map.of());
    }
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

  @Tool(name = "produceMessage", description = """
      Produce (send) a message to a Kafka topic.

      Parameters:
      - topic: Topic name to send the message to (required)
      - value: Message value/payload (required)
      - format: Value format (required). Valid values:
        * "JSON" - Value will be validated as JSON and formatted
        * "STRING" - Value will be sent as plain string
      - key: Optional message key (used for partitioning and message ordering within partition)
      - headers: Optional map of header key-value pairs (e.g., {"source": "api", "version": "v1"})
      - partition: Optional specific partition number (if not specified, Kafka uses key hash or round-robin)

      Returns: ProduceResult object containing:
      - topic: Topic name where message was sent
      - partition: Partition number where message was stored
      - offset: Offset within the partition
      - timestamp: Message timestamp assigned by Kafka

      Examples:
      - Send JSON message with key:
        {topic: "orders", value: "{\\"orderId\\": 123, \\"status\\": \\"pending\\"}", format: "JSON", key: "order-123"}
      - Send plain text message:
        {topic: "logs", value: "Application started", format: "STRING", key: null}
      - Send message with headers:
        {topic: "events", value: "{\\"event\\": \\"user.login\\"}", format: "JSON", key: "user-456", headers: {"source": "auth-service", "correlationId": "abc-123"}}
      """)
  public ProduceResult produceMessage(ProduceRequest request) {
    if (request == null) {
      throw Problems.invalidArgument("Produce request is required", Map.of());
    }
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

  @Tool(name = "tailTopic", description = """
      Tail (read) messages from a Kafka topic with flexible positioning.

      Parameters:
      - topic: Topic name (required)
      - partition: Partition number, or null to read from ALL partitions and merge by timestamp (recommended)
      - from: Starting position - supports multiple formats:
        * "earliest" - Start from the beginning of the topic
        * "latest" - Start from the end (most recent)
        * "end-N" - Last N messages (e.g., "end-100" for last 100 messages)
        * "offset:X" - Start from specific offset X (e.g., "offset:1000")
        * "timestamp:T" - Start from Unix timestamp in milliseconds (e.g., "timestamp:1609459200000")
      - limit: Maximum number of messages to return (default and max configured server-side)
      - startTimestamp: Optional Unix timestamp in ms to filter messages (inclusive start)
      - endTimestamp: Optional Unix timestamp in ms to filter messages (exclusive end)

      Returns: List of MessageEnvelope objects containing:
      - key: Message key (nullable)
      - headers: Map of header key-value pairs
      - valueString: Message value as string
      - valueJson: Message value parsed as JSON (if valid JSON, otherwise null)
      - timestamp: Message timestamp in Unix milliseconds
      - partition: Partition number
      - offset: Offset within the partition

      Examples:
      - Read last 10 messages from all partitions: {topic: "orders", partition: null, from: "end-10", limit: 10}
      - Read from beginning of partition 0: {topic: "orders", partition: 0, from: "earliest", limit: 100}
      - Read recent messages from all partitions: {topic: "orders", partition: null, from: "latest", limit: 50}
      """)
  public List<MessageEnvelope> tailTopic(TailRequest request) {
    if (request == null) {
      throw Problems.invalidArgument("Tail request is required", Map.of());
    }
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
  public List<Map<String, Object>> listConsumerGroups(ListConsumerGroupsInput input) {
    String prefix = input != null ? input.prefix() : null;
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

  @Tool(name = "describeConsumerGroup", description = "Describe a consumer group with partition assignments and lag details")
  public ConsumerGroupDetail describeConsumerGroup(DescribeConsumerGroupInput input) {
    if (input == null) {
      throw Problems.invalidArgument("DescribeConsumerGroupInput is required", Map.of());
    }
    return invoke(
        "describeConsumerGroup",
        () -> describeConsumerGroupUseCase.execute(input.groupId()),
        detail -> new ToolTelemetry(
            detail.lag().size(),
            0L,
            contextOf(
                "groupId", detail.groupId(),
                "state", detail.state(),
                "memberCount", detail.members().size(),
                "partitionCount", detail.lag().size()
            )
        ),
        contextOf("groupId", input.groupId())
    );
  }

  @Tool(name = "searchMessages", description = """
      Search for messages in a Kafka topic containing a keyword. Searches across all partitions with performance safeguards.

      Parameters:
      - topic: Topic name to search in (required)
      - searchTerm: Keyword to search for (required)
      - searchIn: Array of search targets where to look for the keyword (required). Valid values: ["KEY", "VALUE", "HEADERS"]
        * "KEY" - Search in message keys
        * "VALUE" - Search in message values (most common)
        * "HEADERS" - Search in both header names AND header values
        * You can specify multiple targets, e.g., ["VALUE", "HEADERS"]
      - from: Starting position (same format as tailTopic: "earliest", "latest", "end-N", "offset:X", "timestamp:T")
      - limit: Maximum number of matching messages to return (default: 100, configurable server-side)
      - maxScan: Maximum number of messages to scan before stopping (default: 10000, prevents excessive scanning)
      - caseSensitive: Whether search is case-sensitive (default: false, case-insensitive recommended)
      - startTimestamp: Optional Unix timestamp in ms to filter messages (inclusive start)
      - endTimestamp: Optional Unix timestamp in ms to filter messages (exclusive end)

      Returns: SearchResult object containing:
      - messages: Array of matching MessageEnvelope objects (same structure as tailTopic)
      - messagesScanned: Total number of messages examined
      - limitReached: true if stopped because limit was hit (more matches may exist)
      - maxScanReached: true if stopped because maxScan was hit (use timestamp filters or increase maxScan)
      - searchDurationMs: How long the search took in milliseconds

      Performance Notes:
      - Searches ALL partitions by default
      - Uses streaming approach to avoid memory issues
      - Stops early when limit or maxScan is reached
      - For large topics, consider using timestamp filters or "from: latest" to reduce scan scope

      Examples:
      - Search for "NullPointerException" in error topic values:
        {topic: "orders-dlt", searchTerm: "NullPointerException", searchIn: ["VALUE"], from: "earliest", limit: 100, maxScan: 10000, caseSensitive: false}
      - Search for "user-123" in keys and values:
        {topic: "events", searchTerm: "user-123", searchIn: ["KEY", "VALUE"], from: "earliest", limit: 50, maxScan: 5000, caseSensitive: false}
      - Search recent messages for error code in headers:
        {topic: "logs", searchTerm: "ERROR_CODE", searchIn: ["HEADERS"], from: "end-1000", limit: 20, maxScan: 1000, caseSensitive: true}
      """)
  public SearchResult searchMessages(SearchRequest request) {
    if (request == null) {
      throw Problems.invalidArgument("Search request is required", Map.of());
    }
    return invoke(
        "searchMessages",
        () -> searchMessagesUseCase.execute(request),
        result -> new ToolTelemetry(
            result.messages().size(),
            estimateMessageBytes(result.messages()),
            contextOf(
                "topic", request.topic(),
                "messagesScanned", result.messagesScanned(),
                "resultsFound", result.messages().size(),
                "limitReached", result.limitReached(),
                "maxScanReached", result.maxScanReached(),
                "durationMs", result.searchDurationMs()
            )
        ),
        contextOf(
            "topic", request.topic(),
            "searchTerm", request.searchTerm(),
            "searchIn", request.searchIn(),
            "limit", request.limit(),
            "maxScan", request.maxScan()
        )
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

  public record ListTopicsInput(String prefix, String suffix) {}

  public record DescribeTopicInput(String topic) {}

  public record ListConsumerGroupsInput(String prefix) {}

  public record DescribeConsumerGroupInput(String groupId) {}

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
