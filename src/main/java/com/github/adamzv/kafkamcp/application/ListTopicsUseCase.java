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
    if (prefix != null && suffix != null) {
      throw Problems.invalidArgument(
          "Cannot filter by both prefix and suffix",
          Map.of("prefix", prefix, "suffix", suffix)
      );
    }

    Set<String> topicNames = adminPort.listTopicNames();
    List<String> filtered = topicNames.stream()
        .filter(name -> {
          if (prefix != null) {
            return name.startsWith(prefix);
          } else if (suffix != null) {
            return name.endsWith(suffix);
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
