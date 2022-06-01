package org.astraea.admin;

import java.util.List;
import java.util.Map;

/** used to migrate partitions to another broker or broker folder. */
public interface ReplicaMigrator {
  /**
   * move all partitions (leader replica and follower replicas) of topic
   *
   * @param topic topic name
   * @return this migrator
   */
  ReplicaMigrator topic(String topic);

  /**
   * move one partition (leader replica and follower replicas) of topic
   *
   * @param topic topic name
   * @param partition partition id
   * @return this migrator
   */
  ReplicaMigrator partition(String topic, int partition);

  /**
   * move partitions to specify brokers. Noted that this method won't invoke leader election
   * explicitly.
   *
   * @param brokers to host partitions
   */
  default void moveTo(List<Integer> brokers) {
    moveTo(brokers, false);
  }

  /**
   * move partitions to specify brokers. Noted that this method will invoke leader election
   * explicitly if doElection is enabled and, the replica list has at least two replicas.
   *
   * @param brokers to host partitions
   * @param doElection indicates to perform the leader election
   */
  void moveTo(List<Integer> brokers, boolean doElection);

  /**
   * move partition to specify folder.
   *
   * @param brokerFolders contain
   */
  void moveTo(Map<Integer, String> brokerFolders);
}
