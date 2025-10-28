package com.github.adamzv.kafkamcp.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.adamzv.kafkamcp.domain.ProblemCodes;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.domain.TopicInfo;
import com.github.adamzv.kafkamcp.ports.KafkaAdminPort;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ListTopicsUseCaseTest {

  @Test
  void filtersByPrefixAndSorts() {
    KafkaAdminPort adminPort = new KafkaAdminPort() {
      @Override
      public Set<String> listTopicNames() {
        return Set.of("kafka-demo", "alpha", "kafka-metrics");
      }

      @Override
      public Map<String, TopicInfo> describeTopics(Collection<String> topicNames) {
        return topicNames.stream().collect(
            java.util.stream.Collectors.toMap(
                name -> name,
                name -> new TopicInfo(name, 1, (short) 1, false)
            )
        );
      }

      @Override
      public List<Map<String, Object>> listConsumerGroups(String prefix) {
        return List.of();
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

    ListTopicsUseCase useCase = new ListTopicsUseCase(adminPort);
    List<TopicInfo> result = useCase.execute("kafka-", null);

    assertEquals(2, result.size());
    assertEquals("kafka-demo", result.getFirst().name());
    assertEquals("kafka-metrics", result.get(1).name());
  }

  @Test
  void filtersBySuffixAndSorts() {
    KafkaAdminPort adminPort = new KafkaAdminPort() {
      @Override
      public Set<String> listTopicNames() {
        return Set.of("topic", "topic-1", "topic-2", "topic-dlt", "topic-2-dlt", "other");
      }

      @Override
      public Map<String, TopicInfo> describeTopics(Collection<String> topicNames) {
        return topicNames.stream().collect(
            java.util.stream.Collectors.toMap(
                name -> name,
                name -> new TopicInfo(name, 1, (short) 1, false)
            )
        );
      }

      @Override
      public List<Map<String, Object>> listConsumerGroups(String prefix) {
        return List.of();
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

    ListTopicsUseCase useCase = new ListTopicsUseCase(adminPort);
    List<TopicInfo> result = useCase.execute(null, "-dlt");

    assertEquals(2, result.size());
    assertEquals("topic-2-dlt", result.getFirst().name());
    assertEquals("topic-dlt", result.get(1).name());
  }

  @Test
  void throwsWhenBothPrefixAndSuffixProvided() {
    KafkaAdminPort adminPort = new KafkaAdminPort() {
      @Override
      public Set<String> listTopicNames() {
        return Set.of("topic");
      }

      @Override
      public Map<String, TopicInfo> describeTopics(Collection<String> topicNames) {
        return Map.of();
      }

      @Override
      public List<Map<String, Object>> listConsumerGroups(String prefix) {
        return List.of();
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

    ListTopicsUseCase useCase = new ListTopicsUseCase(adminPort);
    ProblemException exception = assertThrows(
        ProblemException.class,
        () -> useCase.execute("prefix-", "-suffix")
    );
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void throwsWhenDescriptionMissing() {
    KafkaAdminPort adminPort = new KafkaAdminPort() {
      @Override
      public Set<String> listTopicNames() {
        return Set.of("demo");
      }

      @Override
      public Map<String, TopicInfo> describeTopics(Collection<String> topicNames) {
        return Map.of();
      }

      @Override
      public List<Map<String, Object>> listConsumerGroups(String prefix) {
        return List.of();
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

    ListTopicsUseCase useCase = new ListTopicsUseCase(adminPort);
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(null, null));
    assertEquals(ProblemCodes.OPERATION_FAILED, exception.problem().code());
  }
}
