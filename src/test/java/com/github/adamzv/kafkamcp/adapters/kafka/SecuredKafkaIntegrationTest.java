package com.github.adamzv.kafkamcp.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.domain.TailRequest;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.support.ApplicationConfig;
import com.github.adamzv.kafkamcp.support.KafkaProperties;
import com.github.adamzv.kafkamcp.support.LimitsProperties;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

/**
 * Integration tests for Kafka MCP server with SASL_PLAIN secured Kafka cluster.
 * This test verifies that all security configuration is applied correctly and
 * all MCP tools work with authenticated connections.
 */
@Testcontainers
@Tag("integration")
class SecuredKafkaIntegrationTest {

  private static final String SASL_USERNAME = "kafka-mcp-user";
  private static final String SASL_PASSWORD = "kafka-mcp-secret";
  private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.5.0");

  @Container
  static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE)
      .withReuse(false)
      .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
          "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT")
      .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
      .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAINTEXT")
      .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN")
      .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
          String.format(
              "org.apache.kafka.common.security.plain.PlainLoginModule required " +
              "username=\"admin\" password=\"admin-secret\" " +
              "user_admin=\"admin-secret\" " +
              "user_%s=\"%s\";",
              SASL_USERNAME, SASL_PASSWORD
          ));

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

    // Configure security with SASL PLAIN
    KafkaProperties kafkaProperties = new KafkaProperties(
        KAFKA.getBootstrapServers(),
        new KafkaProperties.SecurityConfig(
            true,  // Enable security
            new KafkaProperties.SaslConfig(
                "PLAIN",
                null,  // Let it generate JAAS config from username/password
                SASL_USERNAME,
                SASL_PASSWORD
            ),
            new KafkaProperties.SslConfig(null, null, null, null, null, null)
        )
    );

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
  void listTopics_withSaslAuth_succeeds() throws Exception {
    // Create a test topic
    String topic = "secured-test-topic";
    adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    // List topics through adapter
    Set<String> topics = adminAdapter.listTopicNames();

    assertNotNull(topics);
    assertTrue(topics.contains(topic), "Should list the created topic");
  }

  @Test
  void describeTopic_withSaslAuth_succeeds() throws Exception {
    String topic = "secured-describe-topic";
    adminClient.createTopics(List.of(new NewTopic(topic, 3, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    TopicDescriptionResult result = adminAdapter.describeTopic(topic);

    assertNotNull(result);
    assertEquals(topic, result.topic());
    assertEquals(3, result.partitions().size());
  }

  @Test
  void produceAndTail_withSaslAuth_roundTripSucceeds() throws Exception {
    String topic = "secured-roundtrip-topic";
    adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    // Produce a message
    ProduceRequest produceRequest = new ProduceRequest(
        topic,
        "raw-string",
        "test-key",
        Map.of("x-trace-id", "12345"),
        "Secured message content"
    );

    ProduceResult produceResult = producerAdapter.produce(produceRequest);
    assertNotNull(produceResult);
    assertEquals(topic, produceResult.topic());

    // Wait for message to be available
    Thread.sleep(500);

    // Tail the message
    TailRequest tailRequest = new TailRequest(topic, "earliest", null, null);
    List<MessageEnvelope> messages = consumerAdapter.tail(tailRequest, limits);

    assertNotNull(messages);
    assertFalse(messages.isEmpty(), "Should retrieve at least one message");

    MessageEnvelope message = messages.get(0);
    assertEquals("test-key", message.key());
    assertEquals("Secured message content", message.valueString());
    assertTrue(message.headers().containsKey("x-trace-id"));
    assertEquals("12345", message.headers().get("x-trace-id"));
  }

  @Test
  void multipleOperations_withSaslAuth_allSucceed() throws Exception {
    String topic = "secured-multi-ops-topic";
    adminClient.createTopics(List.of(new NewTopic(topic, 2, (short) 1)))
        .all()
        .get(10, TimeUnit.SECONDS);

    // Produce multiple messages
    for (int i = 0; i < 5; i++) {
      ProduceRequest request = new ProduceRequest(
          topic,
          "raw-string",
          "key-" + i,
          Map.of("index", String.valueOf(i)),
          "Message " + i
      );
      producerAdapter.produce(request);
    }

    // Wait for messages
    Thread.sleep(500);

    // List topics
    Set<String> topics = adminAdapter.listTopicNames();
    assertTrue(topics.contains(topic));

    // Describe topic
    TopicDescriptionResult description = adminAdapter.describeTopic(topic);
    assertEquals(2, description.partitions().size());

    // Tail messages
    TailRequest tailRequest = new TailRequest(topic, "earliest", null, null);
    List<MessageEnvelope> messages = consumerAdapter.tail(tailRequest, limits);

    assertEquals(5, messages.size(), "Should retrieve all 5 messages");
  }

  @Test
  void consumerGroups_withSaslAuth_succeed() throws Exception {
    // List consumer groups (may be empty initially)
    // Note: Pass empty string instead of null since Map.of() doesn't accept null values
    List<Map<String, Object>> groups = adminAdapter.listConsumerGroups("");
    assertNotNull(groups);

    // This test just verifies that the operation doesn't fail with authentication
    // Consumer groups may or may not exist depending on previous tests
  }
}
