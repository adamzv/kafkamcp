package com.github.adamzv.kafkamcp.application;

import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.ports.KafkaAdminPort;
import org.springframework.stereotype.Component;
import java.util.Map;

@Component
public class DescribeTopicUseCase {

  private final KafkaAdminPort adminPort;

  public DescribeTopicUseCase(KafkaAdminPort adminPort) {
    this.adminPort = adminPort;
  }

  public TopicDescriptionResult execute(String topic) {
    if (topic == null || topic.isBlank()) {
      throw Problems.invalidArgument("Topic name must be provided", Map.of("topic", topic));
    }

    TopicDescriptionResult description = adminPort.describeTopic(topic);
    if (description == null) {
      throw Problems.operationFailed("Topic description missing", Map.of("topic", topic));
    }
    return description;
  }
}
