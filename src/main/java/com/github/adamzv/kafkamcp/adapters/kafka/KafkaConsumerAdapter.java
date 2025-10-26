package com.github.adamzv.kafkamcp.adapters.kafka;

import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.TailRequest;
import com.github.adamzv.kafkamcp.ports.KafkaConsumerPort;
import com.github.adamzv.kafkamcp.support.KafkaProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerAdapter implements KafkaConsumerPort {

  private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(300);
  private static final Duration CALL_TIMEOUT = Duration.ofSeconds(10);

  private final AdminClient adminClient;
  private final KafkaProperties kafkaProperties;

  public KafkaConsumerAdapter(AdminClient adminClient, KafkaProperties kafkaProperties) {
    this.adminClient = adminClient;
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public List<MessageEnvelope> tail(TailRequest request, Limits limits) {
    List<TopicPartition> partitions = resolvePartitions(request);
    if (partitions.isEmpty()) {
      return List.of();
    }

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-mcp-tail-" + UUID.randomUUID());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-mcp-tail-" + UUID.randomUUID());

    try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.assign(partitions);
      applyStartingPosition(consumer, partitions, request.from());
      return pollMessages(consumer, request, limits);
    } catch (KafkaException ex) {
      throw Problems.kafkaUnavailable(
          "Kafka consume failed",
          Map.of(
              "topic", request.topic(),
              "bootstrapServers", kafkaProperties.bootstrapServers(),
              "error", ex.getClass().getSimpleName(),
              "message", ex.getMessage()
          )
      );
    }
  }

  private List<TopicPartition> resolvePartitions(TailRequest request) {
    DescribeTopicsOptions options = new DescribeTopicsOptions()
        .timeoutMs(Math.toIntExact(ADMIN_TIMEOUT.toMillis()))
        .includeAuthorizedOperations(false);

    DescribeTopicsResult result = adminClient.describeTopics(List.of(request.topic()), options);
    Map<String, TopicDescription> descriptions = await(
        result.all(),
        "describeTopicForTail",
        Map.of("topic", request.topic())
    );

    TopicDescription description = descriptions.get(request.topic());
    if (description == null) {
      throw Problems.notFound("Topic not found", Map.of("topic", request.topic()));
    }

    if (request.partition() != null) {
      int partition = request.partition();
      boolean exists = description.partitions().stream().anyMatch(info -> info.partition() == partition);
      if (!exists) {
        throw Problems.notFound(
            "Topic partition not found",
            Map.of("topic", request.topic(), "partition", partition)
        );
      }
      return List.of(new TopicPartition(request.topic(), partition));
    }

    return description.partitions().stream()
        .map(info -> new TopicPartition(description.name(), info.partition()))
        .toList();
  }

  private void applyStartingPosition(Consumer<String, String> consumer,
                                     List<TopicPartition> partitions,
                                     String from) {
    if (from.startsWith("end-")) {
      long count = Long.parseLong(from.substring(4));
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
      for (TopicPartition tp : partitions) {
        long latest = endOffsets.getOrDefault(tp, 0L);
        long target = Math.max(0, latest - count);
        consumer.seek(tp, target);
      }
      return;
    }

    if (from.startsWith("offset:")) {
      long offset = Long.parseLong(from.substring("offset:".length()));
      for (TopicPartition tp : partitions) {
        consumer.seek(tp, offset);
      }
      return;
    }

    if (from.startsWith("timestamp:")) {
      long timestamp = Long.parseLong(from.substring("timestamp:".length()));
      Map<TopicPartition, Long> timestamps = new HashMap<>();
      for (TopicPartition tp : partitions) {
        timestamps.put(tp, timestamp);
      }
      Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestamps);
      for (TopicPartition tp : partitions) {
        OffsetAndTimestamp data = offsets.get(tp);
        if (data != null) {
          consumer.seek(tp, data.offset());
        } else {
          consumer.seekToEnd(List.of(tp));
        }
      }
      return;
    }

    throw Problems.invalidArgument("Unrecognized from value", Map.of("from", from));
  }

  private List<MessageEnvelope> pollMessages(Consumer<String, String> consumer,
                                             TailRequest request,
                                             Limits limits) {
    List<MessageEnvelope> messages = new ArrayList<>();
    long bytesBudget = limits.bytesPerCall();
    int messageBudget = Math.min(limits.messagesPerCall(), request.limit() == null ? limits.messagesPerCall() : request.limit());
    long deadline = System.nanoTime() + CALL_TIMEOUT.toNanos();
    long bytesUsed = 0;

    while (System.nanoTime() < deadline
        && messages.size() < messageBudget
        && bytesUsed < bytesBudget) {
      ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
      if (records.isEmpty()) {
        continue;
      }
      for (ConsumerRecord<String, String> record : records) {
        if (messages.size() >= messageBudget || System.nanoTime() >= deadline) {
          break;
        }
        String value = record.value();
        long estimatedBytes = value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
        if (!messages.isEmpty() && bytesUsed + estimatedBytes > bytesBudget) {
          return List.copyOf(messages);
        }
        if (bytesUsed + estimatedBytes > bytesBudget && messages.isEmpty()) {
          // include the oversize record but stop afterwards
          bytesUsed = bytesBudget;
        } else {
          bytesUsed += estimatedBytes;
        }

        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
          byte[] valueBytes = header.value();
          headers.put(header.key(), valueBytes == null ? null : new String(valueBytes, StandardCharsets.UTF_8));
        }

        messages.add(new MessageEnvelope(
            record.key(),
            Map.copyOf(headers),
            value,
            null,
            record.timestamp(),
            record.partition(),
            record.offset()
        ));

        if (messages.size() >= messageBudget) {
          break;
        }
        if (bytesUsed >= bytesBudget) {
          break;
        }
      }
    }

    return List.copyOf(messages);
  }

  private <T> T await(KafkaFuture<T> future, String operation, Map<String, Object> context) {
    try {
      return future.get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw Problems.operationFailed("Interrupted while executing " + operation, context);
    } catch (TimeoutException ex) {
      throw Problems.kafkaUnavailable(
          "Timed out contacting Kafka during " + operation,
          mergeContext(context, "TimeoutException", ex.getMessage())
      );
    } catch (ExecutionException ex) {
      throw translate(operation, ex.getCause(), context);
    }
  }

  private ProblemException translate(String operation, Throwable cause, Map<String, Object> context) {
    if (cause instanceof UnknownTopicOrPartitionException) {
      return Problems.notFound(
          "Kafka topic not found during " + operation,
          mergeContext(context, cause.getClass().getSimpleName(), cause.getMessage())
      );
    }
    if (cause instanceof KafkaException) {
      return Problems.kafkaUnavailable(
          "Kafka operation failed: " + operation,
          mergeContext(context, cause.getClass().getSimpleName(), cause.getMessage())
      );
    }
    return Problems.operationFailed(
        "Unexpected failure during " + operation,
        mergeContext(context, cause.getClass().getSimpleName(), cause.getMessage())
    );
  }

  private Map<String, Object> mergeContext(Map<String, Object> base, String error, String message) {
    Map<String, Object> merged = new HashMap<>(base);
    merged.put("bootstrapServers", kafkaProperties.bootstrapServers());
    merged.put("error", error);
    merged.put("message", message);
    return Map.copyOf(merged);
  }
}
