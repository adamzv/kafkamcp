package com.github.adamzv.kafkamcp.adapters.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.adamzv.kafkamcp.domain.Problem;
import java.util.LinkedHashMap;
import java.util.Map;

public class ToolProblemException extends RuntimeException {

  private final Problem problem;

  public ToolProblemException(Problem problem, Throwable cause, ObjectMapper objectMapper) {
    super(problem != null ? serialize(problem, objectMapper) : null, cause);
    this.problem = problem;
  }

  public Problem problem() {
    return problem;
  }

  private static String serialize(Problem problem, ObjectMapper objectMapper) {
    Map<String, Object> error = new LinkedHashMap<>();
    error.put("code", problem.code());
    error.put("message", problem.message());
    if (problem.details() != null && !problem.details().isEmpty()) {
      error.put("details", problem.details());
    }
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("error", error);
    try {
      ObjectWriter writer = objectMapper.writer();
      return writer.writeValueAsString(payload);
    } catch (Exception ex) {
      return problem.code() + ": " + problem.message();
    }
  }
}
