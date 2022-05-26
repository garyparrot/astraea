package org.astraea.balancer.executor;

public interface RebalanceExecutionResult {

  boolean isDone();

  static RebalanceExecutionResult done() {
    return new RebalanceExecutionResult() {
      @Override
      public boolean isDone() {
        return true;
      }
    };
  }

  static RebalanceExecutionResult failed() {
    return new RebalanceExecutionResult() {
      @Override
      public boolean isDone() {
        return false;
      }
    };
  }
}
