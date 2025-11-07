package com.github.adamzv.kafkamcp.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.adamzv.kafkamcp.domain.ConsumerGroupDetail;
import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.domain.SearchRequest;
import com.github.adamzv.kafkamcp.domain.SearchResult;
import com.github.adamzv.kafkamcp.domain.SearchTarget;
import com.github.adamzv.kafkamcp.domain.TailRequest;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.domain.TopicPartitionDetail;
import com.github.adamzv.kafkamcp.support.ApplicationConfig;
import com.github.adamzv.kafkamcp.support.KafkaProperties;
import com.github.adamzv.kafkamcp.support.LimitsProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Tag("integration")
class KafkaAdaptersIntegrationTest {

  private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.5.0");

  @Container
  static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE)
      .withReuse(false);

  private static AdminClient adminClient;
  private static Producer<String, String> producer;
  private static KafkaAdminAdapter adminAdapter;
  private static KafkaProducerAdapter producerAdapter;
  private static KafkaConsumerAdapter consumerAdapter;
  private static Limits limits;

  @BeforeAll
  static void setUp() {
    LimitsProperties limitsProperties = new LimitsProperties(200, 1_000_000, 262_144, 100, 10000);
    ApplicationConfig config = new ApplicationConfig(limitsProperties);
    KafkaProperties kafkaProperties = new KafkaProperties(KAFKA.getBootstrapServers());

    limits = config.limits();
    adminClient = config.adminClient(kafkaProperties);
    producer = config.kafkaProducer(kafkaProperties);

    adminAdapter = new KafkaAdminAdapter(adminClient, kafkaProperties);
    producerAdapter = new KafkaProducerAdapter(producer, kafkaProperties);
    consumerAdapter = new KafkaConsumerAdapter(adminClient, kafkaProperties);
  }

  @AfterAll
  static void tearDown() {
    if (producer != null) {
      producer.close(Duration.ofSeconds(1));
    }
    if (adminClient != null) {
      adminClient.close(Duration.ofSeconds(1));
    }
  }

  @Test
  void produceAndTailRoundTrip() throws Exception {
    String topic = "integration-demo";

    adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    ProduceRequest request = new ProduceRequest(
        topic,
        "raw-string",
        null,
        Map.of("trace-id", "itest"),
        "hello integration"
    );

    ProduceResult result = producerAdapter.produce(request);
    assertEquals(topic, result.topic());

    TailRequest tailRequest = new TailRequest(topic, "end-10", 5, null);
    List<MessageEnvelope> messages = consumerAdapter.tail(tailRequest, limits);

    assertFalse(messages.isEmpty(), "Tail should return at least one message");
    MessageEnvelope first = messages.getFirst();
    assertEquals("hello integration", first.valueString());
    assertEquals("itest", first.headers().get("trace-id"));
  }

  @Test
  void describeTopicIncludesPartitionMetadata() throws Exception {
    String topic = "describe-demo";
    adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    TopicDescriptionResult description = adminAdapter.describeTopic(topic);

    assertEquals(topic, description.topic());
    assertEquals(1, description.partitions().size());
    TopicPartitionDetail partition = description.partitions().getFirst();
    assertEquals(0, partition.partition());
    assertFalse(partition.replicas().isEmpty());
  }

  @Test
  void multiPartitionTailSortsByTimestamp() throws Exception {
    String topic = "multi-partition-demo";
    adminClient.createTopics(List.of(new NewTopic(topic, 3, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    // Produce messages to different partitions with controlled timestamps
    ProduceRequest request1 = new ProduceRequest(topic, "raw-string", "key1", Map.of(), "message-1");
    producerAdapter.produce(request1);
    Thread.sleep(100); // Ensure different timestamps

    ProduceRequest request2 = new ProduceRequest(topic, "raw-string", "key2", Map.of(), "message-2");
    producerAdapter.produce(request2);
    Thread.sleep(100);

    ProduceRequest request3 = new ProduceRequest(topic, "raw-string", "key3", Map.of(), "message-3");
    producerAdapter.produce(request3);

    // Tail from all partitions
    TailRequest tailRequest = new TailRequest(topic, "earliest", 10, null);
    List<MessageEnvelope> messages = consumerAdapter.tail(tailRequest, limits);

    // Should have messages from multiple partitions, sorted by timestamp
    assertTrue(messages.size() >= 3, "Should have at least 3 messages");

    // Verify messages are sorted by timestamp
    for (int i = 1; i < messages.size(); i++) {
      long prevTimestamp = messages.get(i - 1).timestamp();
      long currentTimestamp = messages.get(i).timestamp();
      assertTrue(prevTimestamp <= currentTimestamp,
          "Messages should be sorted by timestamp, but message at index " + (i - 1) +
          " has timestamp " + prevTimestamp + " > message at index " + i +
          " with timestamp " + currentTimestamp);
    }
  }

  @Test
  void describeConsumerGroupReturnsLagInformation() throws Exception {
    String topic = "consumer-group-demo";
    String groupId = "test-consumer-group";

    adminClient.createTopics(List.of(new NewTopic(topic, 2, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    // Produce some messages
    for (int i = 0; i < 5; i++) {
      ProduceRequest request = new ProduceRequest(
          topic,
          "raw-string",
          "key-" + i,
          Map.of(),
          "message-" + i
      );
      producerAdapter.produce(request);
    }

    // Create a consumer and commit offsets
    java.util.Properties consumerProps = new java.util.Properties();
    consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers());
    consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class.getName());
    consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringDeserializer.class.getName());
    consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer =
             new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(List.of(topic));
      // Poll and commit offsets
      consumer.poll(Duration.ofSeconds(5));
      consumer.commitSync();
    }

    // Wait a bit for the consumer group metadata to be available
    Thread.sleep(1000);

    // Now describe the consumer group
    ConsumerGroupDetail detail = adminAdapter.describeConsumerGroup(groupId);

    assertNotNull(detail);
    assertEquals(groupId, detail.groupId());
    assertNotNull(detail.state());
    assertNotNull(detail.lag());

    // Verify lag information exists
    assertFalse(detail.lag().isEmpty(), "Should have lag information for partitions");

    // Check that lag values are calculated correctly
    detail.lag().forEach(lagInfo -> {
      assertEquals(topic, lagInfo.topic());
      assertNotNull(lagInfo.currentOffset(), "Current offset should not be null");
      assertNotNull(lagInfo.endOffset(), "End offset should not be null");
      assertNotNull(lagInfo.lag(), "Lag should not be null");
      assertTrue(lagInfo.lag() >= 0, "Lag should be non-negative");
    });
  }

  @Test
  void searchMessagesFindsKeywordsInComplexJsonPayloads() throws Exception {
    String topic = "search-demo";
    adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    // Load product messages from external JSON file
    List<String> productMessages = loadProductMessagesFromFile("/search_topic.json");
    assertFalse(productMessages.isEmpty(), "Should load product messages from file");
    assertEquals(10, productMessages.size(), "Should load 10 product messages");

    // Produce all messages to the topic
    for (int i = 0; i < productMessages.size(); i++) {
      String message = productMessages.get(i);
      String productId = "product-" + (10001 + i);
      ProduceRequest request = new ProduceRequest(topic, "raw-string", productId, Map.of(), message);
      producerAdapter.produce(request);

      if (i < productMessages.size() - 1) {
        Thread.sleep(50); // Small delay between messages to ensure different timestamps
      }
    }

    Thread.sleep(500); // Wait for all messages to be fully written to Kafka

    // Test 1: Search for "USD" - should find all 10 messages (all have "currency": "USD")
    SearchRequest searchUSD = new SearchRequest(
        topic,
        "USD",
        List.of(SearchTarget.VALUE),
        "earliest",
        100,
        10000,
        true,
        null,
        null
    );
    SearchResult resultUSD = consumerAdapter.search(searchUSD, limits);
    assertEquals(10, resultUSD.messages().size(), "Search for 'USD' should find all 10 product messages");
    assertTrue(resultUSD.messages().stream().allMatch(msg -> msg.valueString().contains("USD")),
        "All messages should contain 'USD'");

    // Test 2: Search for "10001" - should find only the first message (productId: "10001")
    SearchRequest search10001 = new SearchRequest(
        topic,
        "10001",
        List.of(SearchTarget.VALUE),
        "earliest",
        100,
        10000,
        true,
        null,
        null
    );
    SearchResult result10001 = consumerAdapter.search(search10001, limits);
    assertEquals(1, result10001.messages().size(), "Search for '10001' should find only the first product message");
    assertTrue(result10001.messages().get(0).valueString().contains("10001"),
        "Found message should contain '10001'");
    assertTrue(result10001.messages().get(0).valueString().contains("4K Ultra HD Smart TV"),
        "Found message should be the 4K TV product");

    // Test 3: Search for "json" - should find all 10 messages (all have "datacontenttype": "application/json")
    SearchRequest searchJson = new SearchRequest(
        topic,
        "json",
        List.of(SearchTarget.VALUE),
        "earliest",
        100,
        10000,
        false, // case insensitive
        null,
        null
    );
    SearchResult resultJson = consumerAdapter.search(searchJson, limits);
    assertEquals(10, resultJson.messages().size(), "Search for 'json' should find all 10 product messages");
    assertTrue(resultJson.messages().stream().allMatch(msg -> msg.valueString().toLowerCase().contains("json")),
        "All messages should contain 'json'");
  }

  /**
   * Helper method to load product messages from a JSON file in test resources.
   *
   * @param resourcePath Path to the JSON file in test resources
   * @return List of JSON message strings
   */
  private static List<String> loadProductMessagesFromFile(String resourcePath) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    List<String> messages = new ArrayList<>();

    try (InputStream is = KafkaAdaptersIntegrationTest.class.getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }

      JsonNode rootNode = mapper.readTree(is);
      if (!rootNode.isArray()) {
        throw new IOException("Expected JSON array in resource: " + resourcePath);
      }

      for (JsonNode node : rootNode) {
        messages.add(mapper.writeValueAsString(node));
      }
    }

    return messages;
  }
}
