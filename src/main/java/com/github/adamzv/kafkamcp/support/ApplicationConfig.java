package com.github.adamzv.kafkamcp.support;

import com.github.adamzv.kafkamcp.adapters.mcp.KafkaTools;
import com.github.adamzv.kafkamcp.domain.Limits;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({KafkaProperties.class, LimitsProperties.class})
public class ApplicationConfig {

  private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(5);

  private final LimitsProperties limitsProperties;

  public ApplicationConfig(LimitsProperties limitsProperties) {
    this.limitsProperties = limitsProperties;
  }

  @Bean
  public Limits limits() {
    return limitsProperties.toDomain();
  }

  @Bean(destroyMethod = "close")
  public AdminClient adminClient(KafkaProperties kafkaProperties) {
    Properties props = new Properties();
    int timeoutMs = Math.toIntExact(CLIENT_TIMEOUT.toMillis());
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers());
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
    return AdminClient.create(props);
  }

  @Bean(destroyMethod = "close")
  public Producer<String, String> kafkaProducer(KafkaProperties kafkaProperties) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  @Bean
  public ToolCallbackProvider kafkaToolsProvider(KafkaTools kafkaTools) {
    return MethodToolCallbackProvider
        .builder()
        .toolObjects(kafkaTools)
        .build();
  }
}
