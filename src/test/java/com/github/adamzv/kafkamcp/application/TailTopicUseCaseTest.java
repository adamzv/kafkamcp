package com.github.adamzv.kafkamcp.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.ProblemCodes;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.TailRequest;
import com.github.adamzv.kafkamcp.ports.KafkaConsumerPort;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TailTopicUseCaseTest {

  private static final Limits LIMITS = new Limits(200, 1_000_000, 256, 100, 10000);

  private final AtomicReference<TailRequest> captured = new AtomicReference<>();
  private TailTopicUseCase useCase;

  @BeforeEach
  void setUp() {
    KafkaConsumerPort consumerPort = new KafkaConsumerPort() {
      @Override
      public List<MessageEnvelope> tail(TailRequest request, Limits limits) {
        captured.set(request);
        return List.of(new MessageEnvelope(
            "key",
            Map.of(),
            "{\"message\":\"hello\"}",
            null,
            123L,
            0,
            10L
        ));
      }

      @Override
      public com.github.adamzv.kafkamcp.domain.SearchResult search(
          com.github.adamzv.kafkamcp.domain.SearchRequest request,
          Limits limits) {
        return new com.github.adamzv.kafkamcp.domain.SearchResult(List.of(), 0, false, false, 0L);
      }
    };
    useCase = new TailTopicUseCase(consumerPort, new ObjectMapper(), LIMITS);
  }

  @Test
  void defaultsFromAndLimitWhenMissing() {
    TailRequest input = new TailRequest("demo", null, null, null);

    List<MessageEnvelope> result = useCase.execute(input);

    assertEquals(1, result.size());
    assertEquals(0, result.getFirst().partition());
    assertEquals(10L, result.getFirst().offset());
    JsonNode jsonNode = (JsonNode) result.getFirst().valueJson();
    assertEquals("hello", jsonNode.get("message").asText());

    TailRequest normalized = captured.get();
    assertEquals("end-50", normalized.from());
    assertEquals(Integer.valueOf(LIMITS.messagesPerCall()), normalized.limit());
    assertNull(normalized.partition());
  }

  @Test
  void rejectsUnsupportedFrom() {
    TailRequest input = new TailRequest("demo", "bogus", 1, null);
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(input));
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void rejectsNegativeLimit() {
    TailRequest input = new TailRequest("demo", "end-5", -1, null);
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(input));
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void rejectsNegativePartition() {
    TailRequest input = new TailRequest("demo", "end-5", 10, -1);
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(input));
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void acceptsEarliestPosition() {
    TailRequest input = new TailRequest("demo", "earliest", 10, null);
    List<MessageEnvelope> result = useCase.execute(input);

    assertEquals(1, result.size());
    TailRequest normalized = captured.get();
    assertEquals("earliest", normalized.from());
  }

  @Test
  void acceptsLatestPosition() {
    TailRequest input = new TailRequest("demo", "latest", 10, null);
    List<MessageEnvelope> result = useCase.execute(input);

    assertEquals(1, result.size());
    TailRequest normalized = captured.get();
    assertEquals("latest", normalized.from());
  }

  @Test
  void acceptsTimestampPosition() {
    TailRequest input = new TailRequest("demo", "timestamp:1234567890", 10, null);
    List<MessageEnvelope> result = useCase.execute(input);

    assertEquals(1, result.size());
    TailRequest normalized = captured.get();
    assertEquals("timestamp:1234567890", normalized.from());
  }

  @Test
  void rejectsNegativeTimestamp() {
    TailRequest input = new TailRequest("demo", "timestamp:-1", 10, null);
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(input));
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }
}
