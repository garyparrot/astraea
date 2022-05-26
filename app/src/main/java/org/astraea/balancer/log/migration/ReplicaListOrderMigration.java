package org.astraea.balancer.log.migration;

import java.util.List;
import org.astraea.admin.TopicPartition;

/**
 * Alter the order of replica log in the replica list. This operation doesn't promote the
 * first-available log to the leader. One has to use this operation in conjunction with {@link
 * LeadershipMigration} to accomplish this.
 */
@Deprecated
public class ReplicaListOrderMigration implements Migration {

  private final TopicPartition topicPartition;
  private final List<Integer> replicaSet;

  public ReplicaListOrderMigration(TopicPartition topicPartition, List<Integer> replicaSet) {
    this.topicPartition = topicPartition;
    this.replicaSet = List.copyOf(replicaSet);
  }

  @Override
  public TopicPartition topicPartition() {
    return topicPartition;
  }

  public List<Integer> replicaSet() {
    return replicaSet;
  }
}
