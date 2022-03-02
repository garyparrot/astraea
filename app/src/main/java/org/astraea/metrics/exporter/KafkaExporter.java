package org.astraea.metrics.exporter;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
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
        HTTPServer server = new HTTPServer.Builder()
                .withPort(1234)
                .withRegistry(CollectorRegistry.defaultRegistry)
                .build();

        final TopicAdmin admin = TopicAdmin.of("192.168.103.157:25655,192.168.103.158:25655,192.168.103.159:25655");

        final Gauge replicaPath = Gauge.build()
                .name("kafka_tpr")
                .labelNames("topic", "partition", "replica")
                .help("Label")
                .register();

        final Set<TopicPartitionReplica> last = new HashSet<>();

        Executors.newSingleThreadScheduledExecutor()
                        .scheduleAtFixedRate(() -> {
                            try {
                                System.out.printf("[%s] Update%n", LocalDateTime.now());

                                final Set<String> topics = admin.topicNames();
                                final Map<TopicPartition, List<Replica>> replicas = admin.replicas(topics);

                                System.out.printf("[%s] Resource obtained%n", LocalDateTime.now());

                                Set<TopicPartitionReplica> present = new HashSet<>();

                                replicas.forEach((topicPartition, replicaList) -> {
                                    IntStream.range(0, replicaList.size()).forEach(i -> {

                                        final Replica replica = replicaList.get(i);

                                        System.out.printf("[%s] %s %s%n", LocalDateTime.now(),topicPartition, replica.broker());

                                        final var tpr = new TopicPartitionReplica(
                                                topicPartition.topic(),
                                                topicPartition.partition(),
                                                replica.broker());

                                        present.add(tpr);

                                        replicaPath.labels(
                                                tpr.topic(),
                                                Integer.toString(tpr.partition()),
                                                Integer.toString(tpr.brokerId()))
                                                .set(replica.broker());
                                    });
                                });

                                last.stream().filter(x -> !present.contains(x))
                                        .forEach(tpr -> replicaPath.remove(
                                                tpr.topic(),
                                                Integer.toString(tpr.partition()),
                                                Integer.toString(tpr.brokerId())
                                                ));
                                last.clear();
                                last.addAll(present);

                                System.out.printf("[%s] Done%n", LocalDateTime.now());

                            } catch (Exception ex) {
                                System.out.println(ex);
                            }
                        }, 0, 1, TimeUnit.SECONDS);

        // HTTPServer has a daemon thread. Even the main route is finished, this JVM will keep running.
    }

}
