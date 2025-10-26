package com.github.adamzv.kafkamcp.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.adamzv.kafkamcp.domain.ConsumerGroupDetail;
import com.github.adamzv.kafkamcp.domain.ConsumerGroupMember;
import com.github.adamzv.kafkamcp.domain.PartitionAssignment;
import com.github.adamzv.kafkamcp.domain.PartitionLagInfo;
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

class DescribeConsumerGroupUseCaseTest {

  @Test
  void returnsConsumerGroupDetailWithLag() {
    KafkaAdminPort port = new KafkaAdminPort() {
      @Override
      public Set<String> listTopicNames() {
        return Set.of();
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
        return null;
      }

      @Override
      public ConsumerGroupDetail describeConsumerGroup(String groupId) {
        ConsumerGroupMember member = new ConsumerGroupMember(
            "consumer-1",
            "client-1",
            "/127.0.0.1",
            List.of(new PartitionAssignment("test", 0))
        );

        PartitionLagInfo lagInfo = new PartitionLagInfo(
            "test",
            0,
            100L,  // current offset
            150L,  // end offset
            50L    // lag
        );

        return new ConsumerGroupDetail(
            "test-group",
            "Stable",
            List.of(member),
            List.of(lagInfo)
        );
      }
    };

    DescribeConsumerGroupUseCase useCase = new DescribeConsumerGroupUseCase(port);
    ConsumerGroupDetail detail = useCase.execute("test-group");

    assertNotNull(detail);
    assertEquals("test-group", detail.groupId());
    assertEquals("Stable", detail.state());
    assertEquals(1, detail.members().size());
    assertEquals("consumer-1", detail.members().get(0).consumerId());

    assertEquals(1, detail.lag().size());
    PartitionLagInfo lag = detail.lag().get(0);
    assertEquals("test", lag.topic());
    assertEquals(0, lag.partition());
    assertEquals(100L, lag.currentOffset());
    assertEquals(150L, lag.endOffset());
    assertEquals(50L, lag.lag());
  }

  @Test
  void rejectsBlankGroupId() {
    DescribeConsumerGroupUseCase useCase = new DescribeConsumerGroupUseCase(new NoOpAdminPort());
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(" "));
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  @Test
  void rejectsNullGroupId() {
    DescribeConsumerGroupUseCase useCase = new DescribeConsumerGroupUseCase(new NoOpAdminPort());
    ProblemException exception = assertThrows(ProblemException.class, () -> useCase.execute(null));
    assertEquals(ProblemCodes.INVALID_ARGUMENT, exception.problem().code());
  }

  private static class NoOpAdminPort implements KafkaAdminPort {
    @Override
    public Set<String> listTopicNames() {
      return Set.of();
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
      return null;
    }

    @Override
    public ConsumerGroupDetail describeConsumerGroup(String groupId) {
      return null;
    }
  }
}
