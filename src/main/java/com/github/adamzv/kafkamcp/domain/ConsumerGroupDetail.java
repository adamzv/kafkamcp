package com.github.adamzv.kafkamcp.domain;

import java.util.List;

public record ConsumerGroupDetail(
    String groupId,
    String state,
    List<ConsumerGroupMember> members,
    List<PartitionLagInfo> lag
) {
}
