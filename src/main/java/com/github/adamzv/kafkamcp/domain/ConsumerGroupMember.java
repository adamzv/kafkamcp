package com.github.adamzv.kafkamcp.domain;

import java.util.List;

public record ConsumerGroupMember(
    String consumerId,
    String clientId,
    String host,
    List<PartitionAssignment> assignments
) {
}
