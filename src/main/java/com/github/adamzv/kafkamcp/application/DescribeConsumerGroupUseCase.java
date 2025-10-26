package com.github.adamzv.kafkamcp.application;

import com.github.adamzv.kafkamcp.domain.ConsumerGroupDetail;
import com.github.adamzv.kafkamcp.domain.Problems;
import com.github.adamzv.kafkamcp.ports.KafkaAdminPort;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class DescribeConsumerGroupUseCase {

  private final KafkaAdminPort adminPort;

  public DescribeConsumerGroupUseCase(KafkaAdminPort adminPort) {
    this.adminPort = adminPort;
  }

  public ConsumerGroupDetail execute(String groupId) {
    if (groupId == null || groupId.isBlank()) {
      throw Problems.invalidArgument("Group ID must not be blank", Map.of());
    }

    return adminPort.describeConsumerGroup(groupId.trim());
  }
}
