package org.astraea.balancer.log.migration;

import org.astraea.admin.TopicPartition;

/** Ensure the first-available log in the replica list to be the partition leader. */
@Deprecated
public class LeadershipMigration implements Migration {

  private final TopicPartition topicPartition;

  public LeadershipMigration(TopicPartition topicPartition) {
    this.topicPartition = topicPartition;
  }

  @Override
  public TopicPartition topicPartition() {
    return topicPartition;
  }
}
