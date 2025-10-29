package com.github.adamzv.kafkamcp.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.ProblemCodes;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.SearchRequest;
import com.github.adamzv.kafkamcp.domain.SearchResult;
import com.github.adamzv.kafkamcp.domain.SearchTarget;
import com.github.adamzv.kafkamcp.ports.KafkaConsumerPort;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SearchMessagesUseCaseTest {

  private static final Limits LIMITS = new Limits(200, 1_000_000, 256, 100, 10000);

  private SearchMessagesUseCase useCase;

  @BeforeEach
  void setUp() {
    KafkaConsumerPort consumerPort = new KafkaConsumerPort() {
      @Override
      public List<MessageEnvelope> tail(com.github.adamzv.kafkamcp.domain.TailRequest request, Limits limits) {
        return List.of();
      }

      @Override
      public SearchResult search(SearchRequest request, Limits limits) {
        return new SearchResult(List.of(), 0, false, false, 0L);
      }
    };
    useCase = new SearchMessagesUseCase(consumerPort, LIMITS);
  }

  @Test
  void throwsWhenRequestIsNull() {
    ProblemException exception = assertThrows(
        ProblemException.class,
        () -> useCase.execute(null)
    );
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void throwsWhenTopicIsBlank() {
    SearchRequest request = new SearchRequest(
        "",
        "searchTerm",
        List.of(SearchTarget.VALUE),
        null,
        null,
        null,
        null,
        null,
        null
    );

    ProblemException exception = assertThrows(
        ProblemException.class,
        () -> useCase.execute(request)
    );
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void throwsWhenSearchTermIsBlank() {
    SearchRequest request = new SearchRequest(
        "test-topic",
        "",
        List.of(SearchTarget.VALUE),
        null,
        null,
        null,
        null,
        null,
        null
    );

    ProblemException exception = assertThrows(
        ProblemException.class,
        () -> useCase.execute(request)
    );
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void throwsWhenSearchTargetsIsEmpty() {
    SearchRequest request = new SearchRequest(
        "test-topic",
        "searchTerm",
        List.of(),
        null,
        null,
        null,
        null,
        null,
        null
    );

    ProblemException exception = assertThrows(
        ProblemException.class,
        () -> useCase.execute(request)
    );
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void executesSearchWithValidRequest() {
    MessageEnvelope msg1 = new MessageEnvelope(
        "key1",
        Map.of(),
        "test value with error",
        null,
        123456L,
        0,
        0L
    );

    KafkaConsumerPort consumerPort = new KafkaConsumerPort() {
      @Override
      public List<MessageEnvelope> tail(com.github.adamzv.kafkamcp.domain.TailRequest request, Limits limits) {
        return List.of();
      }

      @Override
      public SearchResult search(SearchRequest request, Limits limits) {
        return new SearchResult(
            List.of(msg1),
            100,
            false,
            false,
            50L
        );
      }
    };
    useCase = new SearchMessagesUseCase(consumerPort, LIMITS);

    SearchRequest request = new SearchRequest(
        "test-topic",
        "error",
        List.of(SearchTarget.VALUE),
        null,
        null,
        null,
        null,
        null,
        null
    );

    SearchResult result = useCase.execute(request);

    assertEquals(1, result.messages().size());
    assertEquals(100, result.messagesScanned());
    assertFalse(result.limitReached());
    assertFalse(result.maxScanReached());
    assertEquals("test value with error", result.messages().get(0).valueString());
  }

  @Test
  void passesLimitsToConsumerPort() {
    final Limits[] capturedLimits = new Limits[1];

    KafkaConsumerPort consumerPort = new KafkaConsumerPort() {
      @Override
      public List<MessageEnvelope> tail(com.github.adamzv.kafkamcp.domain.TailRequest request, Limits limits) {
        return List.of();
      }

      @Override
      public SearchResult search(SearchRequest request, Limits limits) {
        capturedLimits[0] = limits;
        return new SearchResult(List.of(), 0, false, false, 0L);
      }
    };
    useCase = new SearchMessagesUseCase(consumerPort, LIMITS);

    SearchRequest request = new SearchRequest(
        "test-topic",
        "search",
        List.of(SearchTarget.VALUE),
        null,
        null,
        null,
        null,
        null,
        null
    );

    useCase.execute(request);

    assertEquals(LIMITS, capturedLimits[0]);
  }
}
