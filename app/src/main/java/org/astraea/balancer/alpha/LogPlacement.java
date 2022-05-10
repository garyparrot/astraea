package org.astraea.balancer.alpha;

import java.util.Optional;

/** This class describe the placement state of one kafka log. */
public interface LogPlacement {

  int broker();

  Optional<String> logDirectory();

  static LogPlacement of(int broker) {
    return of(broker, null);
  }

  static LogPlacement of(int broker, String logDirectory) {
    return new LogPlacement() {
      @Override
      public int broker() {
        return broker;
      }

      @Override
      public Optional<String> logDirectory() {
        return Optional.ofNullable(logDirectory);
      }

      @Override
      public String toString() {
        return "LogPlacement{broker=" + broker() + " logDir=" + logDirectory() + "}";
      }
    };
  }
}
