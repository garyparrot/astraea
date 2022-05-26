package org.astraea.balancer.log.migration;

import org.astraea.admin.TopicPartition;

import java.util.List;

public class ReplicaListMigration implements LogMigration {

    private final TopicPartition topicPartition;
    private final List<Integer> replicaSet;

    public ReplicaListMigration(TopicPartition topicPartition, List<Integer> replicaSet) {
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
