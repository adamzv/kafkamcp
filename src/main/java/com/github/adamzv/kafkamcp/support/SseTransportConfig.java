package com.github.adamzv.kafkamcp.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.server.transport.WebMvcSseServerTransportProvider;
import io.modelcontextprotocol.spec.McpServerTransportProvider;
import org.springframework.ai.mcp.server.autoconfigure.McpServerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "spring.ai.mcp.server", name = "stdio", havingValue = "false")
public class SseTransportConfig {

  @Bean
  public WebMvcSseServerTransportProvider webMvcSseServerTransportProvider(
      ObjectMapper objectMapper,
      McpServerProperties properties) {
    String baseUrl = properties.getBaseUrl();
    if (baseUrl == null || baseUrl.isBlank()) {
      baseUrl = "http://localhost:8080";
    }
    return new WebMvcSseServerTransportProvider(
        objectMapper,
        baseUrl,
        properties.getSseMessageEndpoint(),
        properties.getSseEndpoint()
    );
  }

  @Bean
  @Primary
  public McpServerTransportProvider mcpServerTransportProvider(WebMvcSseServerTransportProvider provider) {
    return provider;
  }

  @Bean
  public RouterFunction<ServerResponse> mcpRouterFunction(WebMvcSseServerTransportProvider provider) {
    return provider.getRouterFunction();
  }
}
