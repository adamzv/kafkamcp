package com.github.adamzv.kafkamcp.application;

import com.github.adamzv.kafkamcp.ports.KafkaAdminPort;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class ListConsumerGroupsUseCase {

  private final KafkaAdminPort adminPort;

  public ListConsumerGroupsUseCase(KafkaAdminPort adminPort) {
    this.adminPort = adminPort;
  }

  public List<Map<String, Object>> execute(String prefix) {
    String normalizedPrefix = (prefix == null || prefix.isBlank()) ? null : prefix;
    List<Map<String, Object>> groups = adminPort.listConsumerGroups(normalizedPrefix);
    if (groups.isEmpty()) {
      return List.of();
    }
    groups = groups.stream().map(Map::copyOf).sorted(
        Comparator.comparing(group -> String.valueOf(group.getOrDefault("groupId", ""))))
        .toList();
    return groups;
  }
}
