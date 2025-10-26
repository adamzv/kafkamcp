package com.github.adamzv.kafkamcp.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.domain.TailRequest;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.domain.TopicPartitionDetail;
import com.github.adamzv.kafkamcp.support.ApplicationConfig;
import com.github.adamzv.kafkamcp.support.KafkaProperties;
import com.github.adamzv.kafkamcp.support.LimitsProperties;
import java.time.Duration;
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
    LimitsProperties limitsProperties = new LimitsProperties(200, 1_000_000, 262_144);
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
}
