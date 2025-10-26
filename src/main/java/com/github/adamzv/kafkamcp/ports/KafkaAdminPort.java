package com.github.adamzv.kafkamcp.ports;

import com.github.adamzv.kafkamcp.domain.ConsumerGroupDetail;
import com.github.adamzv.kafkamcp.domain.TopicDescriptionResult;
import com.github.adamzv.kafkamcp.domain.TopicInfo;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface KafkaAdminPort {

  Set<String> listTopicNames() throws ProblemException;

  Map<String, TopicInfo> describeTopics(Collection<String> topicNames) throws ProblemException;

  List<Map<String, Object>> listConsumerGroups(String prefix) throws ProblemException;

  TopicDescriptionResult describeTopic(String topicName) throws ProblemException;

  ConsumerGroupDetail describeConsumerGroup(String groupId) throws ProblemException;
}
