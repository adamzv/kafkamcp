package com.github.adamzv.kafkamcp.domain;

public class ProblemException extends RuntimeException {

  private final Problem problem;

  public ProblemException(Problem problem) {
    super(problem != null ? problem.message() : null);
    this.problem = problem;
  }

  public Problem problem() {
    return problem;
  }
}
