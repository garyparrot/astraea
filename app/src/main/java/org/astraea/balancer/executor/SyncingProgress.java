package org.astraea.balancer.executor;

import org.astraea.admin.Replica;
import org.astraea.admin.TopicPartition;

/** Monitoring the migration progress of specific replica log */
public interface SyncingProgress {

  static SyncingProgress of(TopicPartition topicPartition, Replica leaderReplica, Replica replica) {
    return new SyncingProgress() {
      @Override
      public TopicPartition topicPartition() {
        return topicPartition;
      }

      @Override
      public int brokerId() {
        return replica.broker();
      }

      @Override
      public boolean synced() {
        return replica.inSync();
      }

      @Override
      public double percentage() {
        // attempts to bypass the divided by zero issue
        if (replica.size() == leaderReplica.size()) {
          return 1;
        } else {
          return (double) replica.size() / leaderReplica.size();
        }
      }

      @Override
      public long logSize() {
        return replica.size();
      }

      @Override
      public long leaderLogSize() {
        return leaderReplica.size();
      }
    };
  }

  /** Current tracking target */
  TopicPartition topicPartition();

  /** Broker of the tracking log */
  int brokerId();

  /** Is the target replica log synced */
  boolean synced();

  /** The ratio between current log size and leader log size */
  double percentage();

  /** The size of current migration log */
  long logSize();

  /** The size of leader log */
  long leaderLogSize();
}
