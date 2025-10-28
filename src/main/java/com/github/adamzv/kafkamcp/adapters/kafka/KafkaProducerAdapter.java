package com.github.adamzv.kafkamcp.adapters.kafka;

import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.ports.KafkaProducerPort;
import com.github.adamzv.kafkamcp.support.KafkaProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducerAdapter implements KafkaProducerPort {

  private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

  private final Producer<String, String> producer;
  private final KafkaProperties kafkaProperties;

  public KafkaProducerAdapter(Producer<String, String> producer, KafkaProperties kafkaProperties) {
    this.producer = producer;
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public ProduceResult produce(ProduceRequest request) {
    ProducerRecord<String, String> record = new ProducerRecord<>(
        request.topic(),
        request.key(),
        request.value()
    );

    if (!request.headers().isEmpty()) {
      request.headers().forEach((key, value) -> {
        byte[] headerValue = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        record.headers().add(new RecordHeader(key, headerValue));
      });
    }

    try {
      RecordMetadata metadata = producer.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      return new ProduceResult(
          metadata.topic(),
          metadata.partition(),
          metadata.offset(),
          metadata.timestamp()
      );
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw Problems.operationFailed("Interrupted while producing", Map.of("topic", request.topic()));
    } catch (TimeoutException ex) {
      throw Problems.kafkaUnavailable(
          "Timed out waiting for Kafka produce acknowledgement",
          Map.of("topic", request.topic(), "bootstrapServers", kafkaProperties.bootstrapServers())
      );
    } catch (ExecutionException ex) {
      throw translateSendFailure(request, ex.getCause());
    }
  }

  private ProblemException translateSendFailure(ProduceRequest request, Throwable cause) {
    if (cause instanceof RecordTooLargeException) {
      return Problems.payloadTooLarge(
          "Kafka rejected message because it exceeds broker limits",
          Map.of("topic", request.topic())
      );
    }
    if (cause instanceof KafkaException) {
      return Problems.kafkaUnavailable(
          "Kafka produce failed",
          buildErrorDetails(request.topic(), cause, true)
      );
    }
    return Problems.operationFailed(
        "Unexpected error during produce",
        buildErrorDetails(request.topic(), cause, false)
    );
  }

  private Map<String, Object> buildErrorDetails(String topic, Throwable cause, boolean includeBootstrap) {
    Map<String, Object> details = new HashMap<>();
    details.put("topic", topic);
    if (includeBootstrap) {
      details.put("bootstrapServers", kafkaProperties.bootstrapServers());
    }
    if (cause != null) {
      details.put("error", cause.getClass().getSimpleName());
      if (cause.getMessage() != null) {
        details.put("message", cause.getMessage());
      }
    }
    return Collections.unmodifiableMap(details);
  }
}
