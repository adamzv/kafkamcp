package com.github.adamzv.kafkamcp.support;

import com.github.adamzv.kafkamcp.domain.Limits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class StartupLogger implements ApplicationListener<ApplicationReadyEvent> {

  private static final Logger log = LoggerFactory.getLogger(StartupLogger.class);

  private final KafkaProperties kafkaProperties;
  private final Limits limits;
  private final Environment environment;

  public StartupLogger(KafkaProperties kafkaProperties, Limits limits, Environment environment) {
    this.kafkaProperties = kafkaProperties;
    this.limits = limits;
    this.environment = environment;
  }

  @Override
  public void onApplicationEvent(ApplicationReadyEvent event) {
    String serverName = environment.getProperty("spring.ai.mcp.server.name", "kafka-mcp");
    String serverVersion = environment.getProperty("spring.ai.mcp.server.version", "unknown");
    log.info(
        "mcp_server_ready name={} version={} bootstrapServers={} limits={{messagesPerCall={}, bytesPerCall={}, messageBytes={}}}",
        serverName,
        serverVersion,
        kafkaProperties.bootstrapServers(),
        limits.messagesPerCall(),
        limits.bytesPerCall(),
        limits.messageBytes()
    );
  }
}
