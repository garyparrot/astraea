package org.astraea.balancer.log.migration;

import org.astraea.admin.TopicPartition;
import org.astraea.utils.DataSize;

public class MoveReplicaMigration implements LogMigration {
    private final TopicPartition topicPartition;
    private final int sourceReplica;
    private final int destinationBroker;

    public MoveReplicaMigration(TopicPartition topicPartition, int sourceReplica, int destinationBroker) {
        this.topicPartition = topicPartition;
        this.sourceReplica = sourceReplica;
        this.destinationBroker = destinationBroker;
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public int sourceReplica() {
        return sourceReplica;
    }

    public int destinationBroker() {
        return destinationBroker;
    }
}
