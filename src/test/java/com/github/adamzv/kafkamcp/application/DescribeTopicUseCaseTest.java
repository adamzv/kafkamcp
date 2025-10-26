package com.github.adamzv.kafkamcp.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.adamzv.kafkamcp.domain.ProblemCodes;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.domain.TopicPartitionDetail;
import com.github.adamzv.kafkamcp.ports.KafkaAdminPort;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DescribeTopicUseCaseTest {

  @Test
  void returnsTopicInfo() {
    KafkaAdminPort port = new KafkaAdminPort() {
      @Override
      public Set<String> listTopicNames() {
        return Set.of("demo");
      }

      @Override
      public Map<String, com.github.adamzv.kafkamcp.domain.TopicInfo> describeTopics(Collection<String> topicNames) {
        return Map.of("demo", new com.github.adamzv.kafkamcp.domain.TopicInfo("demo", 3, (short) 2, false));
      }

      @Override
      public List<Map<String, Object>> listConsumerGroups(String prefix) {
        return List.of();
      }

      @Override
      public TopicDescriptionResult describeTopic(String topicName) {
        return new TopicDescriptionResult(
            "demo",
            false,
            List.of(new TopicPartitionDetail(0, 1, List.of(1, 2), List.of(1, 2)))
        );
      }
    };

    DescribeTopicUseCase useCase = new DescribeTopicUseCase(port);
    TopicDescriptionResult info = useCase.execute("demo");
    assertEquals("demo", info.topic());
    assertEquals(1, info.partitions().size());
    assertEquals(0, info.partitions().getFirst().partition());
  }

  @Test
  void rejectsBlankTopic() {
    DescribeTopicUseCase useCase = new DescribeTopicUseCase(new NoOpAdminPort());
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(" "));
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void throwsWhenTopicMissing() {
    DescribeTopicUseCase useCase = new DescribeTopicUseCase(new NoOpAdminPort());
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute("unknown"));
    assertEquals(ProblemCodes.NOT_FOUND, exception.problem().code());
  }

  private static class NoOpAdminPort implements KafkaAdminPort {
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
      return List.of();
    }

    @Override
    public TopicDescriptionResult describeTopic(String topicName) {
      throw Problems.notFound("Topic not found", Map.of("topic", topicName));
    }
  }
}
