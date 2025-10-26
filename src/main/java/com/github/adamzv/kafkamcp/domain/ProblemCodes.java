package com.github.adamzv.kafkamcp.domain;

public final class ProblemCodes {
  public static final String INVALID_ARGUMENT = "INVALID_ARGUMENT";
  public static final String PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE";
  public static final String NOT_FOUND = "NOT_FOUND";
  public static final String KAFKA_UNAVAILABLE = "KAFKA_UNAVAILABLE";
  public static final String OPERATION_FAILED = "OPERATION_FAILED";

  private ProblemCodes() {
  }
}
