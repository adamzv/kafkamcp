package com.github.adamzv.kafkamcp.domain;

import java.util.Map;

public final class Problems {

  private Problems() {
  }

  public static ProblemException invalidArgument(String message, Map<String, Object> details) {
    return raise(ProblemCodes.INVALID_ARGUMENT, message, details);
  }

  public static ProblemException payloadTooLarge(String message, Map<String, Object> details) {
    return raise(ProblemCodes.PAYLOAD_TOO_LARGE, message, details);
  }

  public static ProblemException notFound(String message, Map<String, Object> details) {
    return raise(ProblemCodes.NOT_FOUND, message, details);
  }

  public static ProblemException kafkaUnavailable(String message, Map<String, Object> details) {
    return raise(ProblemCodes.KAFKA_UNAVAILABLE, message, details);
  }

  public static ProblemException operationFailed(String message, Map<String, Object> details) {
    return raise(ProblemCodes.OPERATION_FAILED, message, details);
  }

  private static ProblemException raise(String code, String message, Map<String, Object> details) {
    return new ProblemException(new Problem(code, message, details));
  }
}
