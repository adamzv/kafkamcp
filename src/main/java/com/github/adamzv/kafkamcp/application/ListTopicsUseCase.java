package com.github.adamzv.kafkamcp.application;

import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.TopicInfo;
import com.github.adamzv.kafkamcp.ports.KafkaAdminPort;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class ListTopicsUseCase {

  private final KafkaAdminPort adminPort;

  public ListTopicsUseCase(KafkaAdminPort adminPort) {
    this.adminPort = adminPort;
  }

  public List<TopicInfo> execute(String prefix, String suffix) {
    // Normalize empty strings to null
    final String normalizedPrefix = (prefix == null || prefix.isBlank()) ? null : prefix;
    final String normalizedSuffix = (suffix == null || suffix.isBlank()) ? null : suffix;

    if (normalizedPrefix != null && normalizedSuffix != null) {
      throw Problems.invalidArgument(
          "Cannot filter by both prefix and suffix",
          Map.of("prefix", normalizedPrefix, "suffix", normalizedSuffix)
      );
    }

    Set<String> topicNames = adminPort.listTopicNames();
    List<String> filtered = topicNames.stream()
        .filter(name -> {
          if (normalizedPrefix != null) {
            return name.startsWith(normalizedPrefix);
          } else if (normalizedSuffix != null) {
            return name.endsWith(normalizedSuffix);
          }
          return true;
        })
        .sorted()
        .toList();

    if (filtered.isEmpty()) {
      return List.of();
    }

    Map<String, TopicInfo> described = adminPort.describeTopics(filtered);
    List<TopicInfo> ordered = new ArrayList<>(filtered.size());
    for (String name : filtered) {
      TopicInfo info = described.get(name);
      if (info == null) {
        throw Problems.operationFailed(
            "Topic description missing from admin port",
            Map.of("topic", name)
        );
      }
      ordered.add(info);
    }
    return List.copyOf(ordered);
  }
}
