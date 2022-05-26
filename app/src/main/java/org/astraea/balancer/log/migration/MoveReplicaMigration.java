package org.astraea.balancer.log.migration;

import java.util.List;
import org.astraea.admin.TopicPartition;

/** Let specific replica log leave the replica list and make another one join */
@Deprecated
public class MoveReplicaMigration implements Migration {
  private final TopicPartition topicPartition;

  private final List<Integer> replicaToJoin;

  private final List<Integer> replicaToLeave;

  public MoveReplicaMigration(
      TopicPartition topicPartition, List<Integer> replicaToJoin, List<Integer> replicaToLeave) {
    this.topicPartition = topicPartition;
    this.replicaToJoin = replicaToJoin;
    this.replicaToLeave = replicaToLeave;
  }

  @Override
  public TopicPartition topicPartition() {
    return topicPartition;
  }

  public List<Integer> replicaToJoin() {
    return replicaToJoin;
  }

  public List<Integer> replicaToLeave() {
    return replicaToLeave;
  }
}
