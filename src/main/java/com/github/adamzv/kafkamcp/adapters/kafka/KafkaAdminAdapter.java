package com.github.adamzv.kafkamcp.adapters.kafka;

import com.github.adamzv.kafkamcp.domain.ConsumerGroupDetail;
import com.github.adamzv.kafkamcp.domain.ConsumerGroupMember;
import com.github.adamzv.kafkamcp.domain.PartitionAssignment;
import com.github.adamzv.kafkamcp.domain.PartitionLagInfo;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.domain.TopicInfo;
import com.github.adamzv.kafkamcp.domain.TopicPartitionDetail;
import com.github.adamzv.kafkamcp.ports.KafkaAdminPort;
import com.github.adamzv.kafkamcp.support.KafkaProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Component;

@Component
public class KafkaAdminAdapter implements KafkaAdminPort {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  private final AdminClient adminClient;
  private final KafkaProperties kafkaProperties;

  public KafkaAdminAdapter(AdminClient adminClient, KafkaProperties kafkaProperties) {
    this.adminClient = adminClient;
    this.kafkaProperties = kafkaProperties;
  }

  @Override
  public Set<String> listTopicNames() {
    ListTopicsOptions options = new ListTopicsOptions()
        .listInternal(true)
        .timeoutMs(Math.toIntExact(DEFAULT_TIMEOUT.toMillis()));

    KafkaFuture<Set<String>> future = adminClient.listTopics(options).names();
    Set<String> names = await(future, "listTopics", Map.of());
    return names.stream().collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public Map<String, TopicInfo> describeTopics(Collection<String> topicNames) {
    if (topicNames == null || topicNames.isEmpty()) {
      return Map.of();
    }

    DescribeTopicsOptions options = new DescribeTopicsOptions()
        .timeoutMs(Math.toIntExact(DEFAULT_TIMEOUT.toMillis()))
        .includeAuthorizedOperations(false);

    DescribeTopicsResult result = adminClient.describeTopics(topicNames, options);
    Map<String, TopicDescription> description = await(
        result.allTopicNames(),
        "describeTopics",
        Map.of("topics", List.copyOf(topicNames))
    );

    Map<String, TopicInfo> mapped = HashMap.newHashMap(description.size());
    for (Map.Entry<String, TopicDescription> entry : description.entrySet()) {
      TopicDescription value = entry.getValue();
      short replication = value.partitions().isEmpty()
          ? 0
          : (short) value.partitions().getFirst().replicas().size();
      TopicInfo info = new TopicInfo(
          value.name(),
          value.partitions().size(),
          replication,
          value.name().startsWith("__")
      );
      mapped.put(entry.getKey(), info);
    }
    return Map.copyOf(mapped);
  }

  @Override
  public List<Map<String, Object>> listConsumerGroups(String prefix) {
    ListConsumerGroupsOptions options = new ListConsumerGroupsOptions()
        .timeoutMs(Math.toIntExact(DEFAULT_TIMEOUT.toMillis()));

    Collection<ConsumerGroupListing> listings = await(
        adminClient.listConsumerGroups(options).all(),
        "listConsumerGroups",
        Map.of("prefix", prefix)
    );

    List<ConsumerGroupListing> filtered = listings.stream()
        .filter(listing -> prefix == null || listing.groupId().startsWith(prefix))
        .toList();
    if (filtered.isEmpty()) {
      return List.of();
    }

    DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(
        filtered.stream().map(ConsumerGroupListing::groupId).toList()
    );

    Map<String, ConsumerGroupDescription> descriptions = await(
        describeResult.all(),
        "describeConsumerGroups",
        Map.of("count", filtered.size())
    );

    List<Map<String, Object>> payload = new ArrayList<>(descriptions.size());
    for (ConsumerGroupDescription description : descriptions.values()) {
      List<Map<String, Object>> members = description.members().stream()
          .map(member -> Map.<String, Object>of(
              "consumerId", member.consumerId(),
              "clientId", member.clientId(),
              "host", member.host()))
          .toList();

      Map<String, Object> entry = Map.of(
          "groupId", description.groupId(),
          "state", description.state().toString(),
          "members", List.copyOf(members)
      );
      payload.add(entry);
    }
    return List.copyOf(payload);
  }

  @Override
  public TopicDescriptionResult describeTopic(String topicName) {
    if (topicName == null || topicName.isBlank()) {
      throw Problems.invalidArgument("Topic name must be provided", Map.of("topic", topicName));
    }

    DescribeTopicsOptions options = new DescribeTopicsOptions()
        .timeoutMs(Math.toIntExact(DEFAULT_TIMEOUT.toMillis()))
        .includeAuthorizedOperations(false);

    DescribeTopicsResult result = adminClient.describeTopics(List.of(topicName), options);
    Map<String, TopicDescription> description = await(
        result.allTopicNames(),
        "describeTopic",
        Map.of("topic", topicName)
    );

    TopicDescription value = description.get(topicName);
    if (value == null) {
      throw Problems.notFound("Topic not found", Map.of("topic", topicName));
    }

    List<TopicPartitionDetail> partitions = value.partitions().stream()
        .map(partition -> new TopicPartitionDetail(
            partition.partition(),
            partition.leader() == null ? null : partition.leader().id(),
            mapNodes(partition.isr()),
            mapNodes(partition.replicas())
        ))
        .toList();

    return new TopicDescriptionResult(
        value.name(),
        value.name().startsWith("__"),
        List.copyOf(partitions)
    );
  }

  @Override
  public ConsumerGroupDetail describeConsumerGroup(String groupId) {
    if (groupId == null || groupId.isBlank()) {
      throw Problems.invalidArgument("Group ID must be provided", Map.of("groupId", groupId));
    }

    // Describe the consumer group
    DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(List.of(groupId));
    Map<String, ConsumerGroupDescription> descriptions = await(
        describeResult.all(),
        "describeConsumerGroup",
        Map.of("groupId", groupId)
    );

    ConsumerGroupDescription description = descriptions.get(groupId);
    if (description == null) {
      throw Problems.notFound("Consumer group not found", Map.of("groupId", groupId));
    }

    // Get committed offsets for the consumer group
    Map<TopicPartition, OffsetAndMetadata> committedOffsets = await(
        adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
        "listConsumerGroupOffsets",
        Map.of("groupId", groupId)
    );

    // Get end offsets for all partitions
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    if (!committedOffsets.isEmpty()) {
      Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
      for (TopicPartition tp : committedOffsets.keySet()) {
        offsetSpecs.put(tp, OffsetSpec.latest());
      }
      ListOffsetsResult offsetsResult = adminClient.listOffsets(offsetSpecs);
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> resultInfo = await(
          offsetsResult.all(),
          "listOffsets",
          Map.of("groupId", groupId)
      );
      for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : resultInfo.entrySet()) {
        endOffsets.put(entry.getKey(), entry.getValue().offset());
      }
    }

    // Build member details with partition assignments
    List<ConsumerGroupMember> members = description.members().stream()
        .map(this::buildMemberDetail)
        .toList();

    // Calculate lag for all partitions
    List<PartitionLagInfo> lagInfo = new ArrayList<>();
    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
      TopicPartition tp = entry.getKey();
      Long currentOffset = entry.getValue() != null ? entry.getValue().offset() : null;
      Long endOffset = endOffsets.get(tp);
      Long lag = (currentOffset != null && endOffset != null) ? endOffset - currentOffset : null;

      lagInfo.add(new PartitionLagInfo(
          tp.topic(),
          tp.partition(),
          currentOffset,
          endOffset,
          lag
      ));
    }

    return new ConsumerGroupDetail(
        description.groupId(),
        description.state().toString(),
        List.copyOf(members),
        List.copyOf(lagInfo)
    );
  }

  private ConsumerGroupMember buildMemberDetail(MemberDescription member) {
    List<PartitionAssignment> assignments = new ArrayList<>();
    if (member.assignment() != null && member.assignment().topicPartitions() != null) {
      for (TopicPartition tp : member.assignment().topicPartitions()) {
        assignments.add(new PartitionAssignment(tp.topic(), tp.partition()));
      }
    }
    return new ConsumerGroupMember(
        member.consumerId(),
        member.clientId(),
        member.host(),
        List.copyOf(assignments)
    );
  }

  private List<Integer> mapNodes(List<Node> nodes) {
    if (nodes == null || nodes.isEmpty()) {
      return List.of();
    }
    return nodes.stream().map(Node::id).toList();
  }

  private <T> T await(KafkaFuture<T> future, String operation, Map<String, Object> context) {
    try {
      return future.get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw Problems.operationFailed("Interrupted while executing " + operation, context);
    } catch (TimeoutException ex) {
      throw Problems.kafkaUnavailable("Timed out contacting Kafka during " + operation, mergedContext(context, ex));
    } catch (ExecutionException ex) {
      throw translate(operation, ex.getCause(), context);
    }
  }

  private ProblemException translate(String operation, Throwable cause, Map<String, Object> context) {
    Map<String, Object> details = mergedContext(context, cause);
    if (cause instanceof UnknownTopicOrPartitionException) {
      return Problems.notFound("Kafka topic not found", details);
    }
    if (cause instanceof KafkaException) {
      return Problems.kafkaUnavailable("Kafka operation failed: " + operation, details);
    }
    return Problems.operationFailed("Unexpected failure during " + operation, details);
  }

  private Map<String, Object> mergedContext(Map<String, Object> context, Throwable cause) {
    Map<String, Object> merged = new HashMap<>(context);
    merged.put("bootstrapServers", kafkaProperties.bootstrapServers());
    merged.put("error", cause.getClass().getSimpleName());
    merged.put("message", cause.getMessage());
    return Map.copyOf(merged);
  }

  private Map<String, Object> mergedContext(Map<String, Object> context, TimeoutException ex) {
    Map<String, Object> merged = new HashMap<>(context);
    merged.put("bootstrapServers", kafkaProperties.bootstrapServers());
    merged.put("error", "TimeoutException");
    merged.put("message", ex.getMessage());
    return Map.copyOf(merged);
  }
}
