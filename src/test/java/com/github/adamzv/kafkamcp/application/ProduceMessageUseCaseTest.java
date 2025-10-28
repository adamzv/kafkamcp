package com.github.adamzv.kafkamcp.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.ProblemCodes;
import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.ports.KafkaProducerPort;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProduceMessageUseCaseTest {

  private static final Limits LIMITS = new Limits(200, 1_000_000, 256, 100, 10000);

  private final AtomicReference<ProduceRequest> captured = new AtomicReference<>();
  private ProduceMessageUseCase useCase;

  @BeforeEach
  void setUp() {
    KafkaProducerPort producerPort = request -> {
      captured.set(request);
      return new ProduceResult(request.topic(), 0, 123L, System.currentTimeMillis());
    };
    useCase = new ProduceMessageUseCase(producerPort, new ObjectMapper(), LIMITS);
  }

  @Test
  void producesMessageWithSanitizedHeaders() {
    ProduceRequest input = new ProduceRequest(
        "demo",
        null,
        "key",
        Map.of("trace-id", "123"),
        "hello"
    );

    ProduceResult result = useCase.execute(input);

    assertEquals("demo", result.topic());
    ProduceRequest sanitized = captured.get();
    assertEquals(ProduceMessageUseCase.FORMAT_RAW, sanitized.format());
    assertEquals(Map.of("trace-id", "123"), sanitized.headers());
  }

  @Test
  void rejectsInvalidJsonWhenFormatJson() {
    ProduceRequest input = new ProduceRequest(
        "demo",
        "json",
        null,
        Map.of(),
        "not-json"
    );

    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(input));
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void rejectsOversizePayload() {
    String oversized = "x".repeat(LIMITS.messageBytes() + 1);
    ProduceRequest input = new ProduceRequest(
        "demo",
        "raw-string",
        null,
        Map.of(),
        oversized
    );

    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(input));
    assertEquals(ProblemCodes.PAYLOAD_TOO_LARGE, exception.problem().code());
  }
}
