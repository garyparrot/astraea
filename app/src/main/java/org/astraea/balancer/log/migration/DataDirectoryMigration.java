package org.astraea.balancer.log.migration;

import org.astraea.admin.TopicPartition;

/** Let specific replica log migrate to specific data directory on the same broker. */
@Deprecated
public class DataDirectoryMigration implements Migration {

  private final TopicPartition topicPartition;
  private final int sourceReplica;
  private final String dataDirectory;

  public DataDirectoryMigration(
      TopicPartition topicPartition, int sourceReplica, String dataDirectory) {
    this.topicPartition = topicPartition;
    this.sourceReplica = sourceReplica;
    this.dataDirectory = dataDirectory;
  }

  @Override
  public TopicPartition topicPartition() {
    return topicPartition;
  }

  public int sourceReplica() {
    return sourceReplica;
  }

  public String dataDirectory() {
    return dataDirectory;
  }
}
