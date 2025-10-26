package com.github.adamzv.kafkamcp.ports;

import com.github.adamzv.kafkamcp.domain.ProduceRequest;
import com.github.adamzv.kafkamcp.domain.ProduceResult;
import com.github.adamzv.kafkamcp.domain.ProblemException;

public interface KafkaProducerPort {

  ProduceResult produce(ProduceRequest request) throws ProblemException;
}
