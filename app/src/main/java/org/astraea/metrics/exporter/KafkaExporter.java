package org.astraea.metrics.exporter;

import io.prometheus.client.*;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.Utils;
import org.astraea.topic.Replica;
import org.astraea.topic.TopicAdmin;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KafkaExporter {

    public static void main(String[] args) throws IOException, InterruptedException {
        final TopicAdmin admin = TopicAdmin.of("192.168.103.157:25655,192.168.103.158:25655,192.168.103.159:25655");

        CollectorRegistry.defaultRegistry.register(new Collector() {
            @Override
            public List<MetricFamilySamples> collect() {
                final var topics = admin.topicNames();
                final var topicReplicas= admin.replicas(topics);

                final var replicaBrokerId = new GaugeMetricFamily(
                        "kafka_tpr",
                        "topic/partition/replica",
                        List.of("topic", "partition", "partition_pretty", "replica"));

                final var replicaLeader = new GaugeMetricFamily(
                        "kafka_tpr_is_leader",
                        "topic/partition/replica is leader or not",
                        List.of("topic", "partition", "partition_pretty", "replica"));

                topicReplicas.forEach((topicPartition, replicaList) -> {
                    replicaList.forEach(replica -> {
                            var labels = List.of(
                                    topicPartition.topic(),
                                    String.format("%d", topicPartition.partition()),
                                    String.format("%05d", topicPartition.partition()),
                                    String.format("%d", replica.broker()));
                            replicaBrokerId.addMetric(labels, replica.broker());
                    });
                });

                topicReplicas.forEach((topicPartition, replicaList) -> {
                    replicaList.forEach(replica -> {
                        var labels = List.of(
                                topicPartition.topic(),
                                String.format("%d", topicPartition.partition()),
                                String.format("%05d", topicPartition.partition()),
                                String.format("%d", replica.broker()));
                        replicaLeader.addMetric(labels, replica.leader() ? 1 : 0);
                    });
                });

                return List.of(replicaBrokerId, replicaLeader);
            }
        });

        // HTTPServer has a non-daemon thread. Even the main route is finished, this JVM will keep running.
        HTTPServer server = new HTTPServer.Builder()
                .withPort(1234)
                .withRegistry(CollectorRegistry.defaultRegistry)
                .withDaemonThreads(false)
                .build();
    }

}