package com.github.adamzv.kafkamcp.ports;

import com.github.adamzv.kafkamcp.domain.Limits;
import com.github.adamzv.kafkamcp.domain.MessageEnvelope;
import com.github.adamzv.kafkamcp.domain.ProblemException;
import com.github.adamzv.kafkamcp.domain.SearchRequest;
import com.github.adamzv.kafkamcp.domain.SearchResult;
import com.github.adamzv.kafkamcp.domain.TailRequest;
import java.util.List;

public interface KafkaConsumerPort {

  List<MessageEnvelope> tail(TailRequest request, Limits limits) throws ProblemException;

  SearchResult search(SearchRequest request, Limits limits) throws ProblemException;
}
