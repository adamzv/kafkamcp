package com.github.adamzv.kafkamcp.application;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.ports.KafkaAdminPort;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ListConsumerGroupsUseCaseTest {

  @Test
  void returnsSortedCopy() {
    KafkaAdminPort adminPort = new KafkaAdminPort() {
      @Override
      public Set<String> listTopicNames() {
        return Set.of();
      }

      @Override
      public Map<String, com.github.adamzv.kafkamcp.domain.TopicInfo> describeTopics(Collection<String> topicNames) {
        return Map.of();
      }

      @Override
      public List<Map<String, Object>> listConsumerGroups(String prefix) {
        List<Map<String, Object>> groups = new ArrayList<>();
        groups.add(Map.of("groupId", "group-b"));
        groups.add(Map.of("groupId", "group-a"));
        return groups;
      }

      @Override
      public TopicDescriptionResult describeTopic(String topicName) {
        throw new UnsupportedOperationException();
      }

      @Override
      public com.github.adamzv.kafkamcp.domain.ConsumerGroupDetail describeConsumerGroup(String groupId) {
        return null;
      }
    };

    ListConsumerGroupsUseCase useCase = new ListConsumerGroupsUseCase(adminPort);
    List<Map<String, Object>> result = useCase.execute(null);

    assertEquals("group-a", result.get(0).get("groupId"));
    assertEquals("group-b", result.get(1).get("groupId"));
  }
}
