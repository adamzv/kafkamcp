package com.github.adamzv.kafkamcp.adapters.kafka;

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
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
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
        result.all(),
        "describeTopics",
        Map.of("topics", List.copyOf(topicNames))
    );

    Map<String, TopicInfo> mapped = new HashMap<>(description.size());
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
        result.all(),
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
