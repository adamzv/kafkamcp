package com.github.adamzv.kafkamcp.application;

import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.SearchRequest;
import com.github.adamzv.kafkamcp.domain.SearchResult;
import com.github.adamzv.kafkamcp.ports.KafkaConsumerPort;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class SearchMessagesUseCase {

  private final KafkaConsumerPort consumerPort;
  private final Limits limits;

  public SearchMessagesUseCase(KafkaConsumerPort consumerPort, Limits limits) {
    this.consumerPort = consumerPort;
    this.limits = limits;
  }

  public SearchResult execute(SearchRequest request) {
    if (request == null) {
      throw Problems.invalidArgument("Search request is required", Map.of());
    }

    if (request.topic() == null || request.topic().isBlank()) {
      throw Problems.invalidArgument("Topic must not be blank", Map.of());
    }

    if (request.searchTerm() == null || request.searchTerm().isBlank()) {
      throw Problems.invalidArgument("Search term must not be blank", Map.of());
    }

    if (request.searchIn() == null || request.searchIn().isEmpty()) {
      throw Problems.invalidArgument("Search targets (searchIn) must not be empty", Map.of());
    }

    return consumerPort.search(request, limits);
  }
}
