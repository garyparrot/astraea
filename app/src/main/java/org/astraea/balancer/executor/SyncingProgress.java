package org.astraea.balancer.executor;

import org.astraea.admin.Replica;
import org.astraea.admin.TopicPartition;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;

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
      public DataSize logSize() {
        return DataUnit.Byte.of(replica.size());
      }

      @Override
      public DataSize leaderLogSize() {
        return DataUnit.Byte.of(leaderReplica.size());
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
  DataSize logSize();

  /** The size of leader log */
  DataSize leaderLogSize();
}
