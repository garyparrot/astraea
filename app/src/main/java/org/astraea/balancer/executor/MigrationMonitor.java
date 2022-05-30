package org.astraea.balancer.executor;

import org.astraea.admin.TopicPartition;
import org.astraea.common.DataSize;

/** Monitoring the migration progress of specific replica log */
public interface MigrationMonitor {

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
