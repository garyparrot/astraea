package org.astraea.balancer.log.migration;

import org.astraea.admin.TopicPartition;

public class DataDirectoryMigration implements LogMigration {

    private final TopicPartition topicPartition;
    private final int sourceReplica;
    private final String dataDirectory;

    public DataDirectoryMigration(TopicPartition topicPartition, int sourceReplica, String dataDirectory) {
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
