package org.astraea.balancer.executor;

import java.util.Optional;

public interface RebalanceExecutionResult {

  boolean isDone();

  Optional<Throwable> exception();

  static RebalanceExecutionResult create(boolean isDone, Throwable exception) {
    return new RebalanceExecutionResult() {
      @Override
      public boolean isDone() {
        return isDone;
      }

      @Override
      public Optional<Throwable> exception() {
        return Optional.ofNullable(exception);
      }
    };
  }

  static RebalanceExecutionResult done() {
    return create(true, null);
  }

  static RebalanceExecutionResult failed(Throwable exception) {
    return create(false, exception);
  }
}
