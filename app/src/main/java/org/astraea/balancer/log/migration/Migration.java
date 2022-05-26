package org.astraea.balancer.log.migration;

import org.astraea.admin.TopicPartition;

/** Data class that describe specific kind of Kafka migration operation. */
@Deprecated
public interface Migration {
  TopicPartition topicPartition();
}
