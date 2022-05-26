package org.astraea.cost.broker;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.admin.TopicPartition;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.PartitionCost;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;

public class ReplicaDiskInCost implements HasBrokerCost {
  Map<Integer, Integer> brokerBandwidthCap;

  public ReplicaDiskInCost(Map<Integer, Integer> brokerBandwidthCap) {
    this.brokerBandwidthCap = brokerBandwidthCap;
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    final Map<Integer, List<TopicPartitionReplica>> topicPartitionOfEachBroker =
        clusterInfo.topics().stream()
            .flatMap(topic -> clusterInfo.replicas(topic).stream())
            .map(
                replica ->
                    new TopicPartitionReplica(
                        replica.topic(), replica.partition(), replica.nodeInfo().id()))
            .collect(Collectors.groupingBy(TopicPartitionReplica::brokerId));
    final var actual =
        clusterInfo.nodes().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    NodeInfo::id,
                    node -> topicPartitionOfEachBroker.getOrDefault(node.id(), List.of())));

    final var topicPartitionDataRate = topicPartitionDataRate(clusterInfo, Duration.ofSeconds(3));

    final var brokerLoad =
        actual.entrySet().stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().stream()
                            .mapToDouble(
                                x ->
                                    topicPartitionDataRate.get(
                                        new TopicPartition(x.topic(), x.partition())))
                            .sum()))
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue() / brokerBandwidthCap.get(entry.getKey()) / 1024 / 1024))
            .map(entry -> Map.entry(entry.getKey(), Math.min(entry.getValue(), 1)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return () -> brokerLoad;
  }

  /**
   * Calculate the maximum increase rate of each topic/partition, across the whole cluster.
   *
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @param sampleWindow the time interval for calculating the data rate, noted that if the metrics
   *     doesn't have the sufficient old metric then an exception will likely be thrown.
   * @return a map contain the maximum increase rate of each topic/partition log
   */
  public static Map<TopicPartition, Double> topicPartitionDataRate(
      ClusterInfo clusterInfo, Duration sampleWindow) {
    return clusterInfo.allBeans().entrySet().parallelStream()
        .map(
            entry ->
                entry.getValue().parallelStream()
                    .filter(bean -> bean instanceof HasValue)
                    .filter(bean -> bean.beanObject().getProperties().get("type").equals("Log"))
                    .filter(bean -> bean.beanObject().getProperties().get("name").equals("Size"))
                    .map(bean -> (HasValue) bean)
                    .collect(
                        Collectors.groupingBy(
                            bean ->
                                TopicPartition.of(
                                    bean.beanObject().getProperties().get("topic"),
                                    String.valueOf(
                                        Integer.parseInt(
                                            bean.beanObject().getProperties().get("partition"))))))
                    .entrySet()
                    .parallelStream()
                    .map(
                        metrics -> {
                          // calculate the increase rate over a specific window of time
                          var sizeTimeSeries =
                              metrics.getValue().stream()
                                  .sorted(
                                      Comparator.comparingLong(HasBeanObject::createdTimestamp)
                                          .reversed())
                                  .collect(Collectors.toUnmodifiableList());
                          var latestSize = sizeTimeSeries.stream().findFirst().orElseThrow();
                          var windowSize =
                              sizeTimeSeries.stream()
                                  .dropWhile(
                                      bean ->
                                          bean.createdTimestamp()
                                              > latestSize.createdTimestamp()
                                                  - sampleWindow.toMillis())
                                  .findFirst()
                                  .orElseThrow(
                                      () ->
                                          new IllegalStateException(
                                              "No sufficient info to determine data rate, try later."));
                          var dataRate =
                              ((double) (latestSize.value() - windowSize.value()))
                                  / ((double)
                                          (latestSize.createdTimestamp()
                                              - windowSize.createdTimestamp())
                                      / 1000);
                          return Map.entry(metrics.getKey(), dataRate);
                        })
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)))
        .flatMap(logSizeMap -> logSizeMap.entrySet().stream())
        .collect(
            Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(
                    Map.Entry::getValue, Collectors.maxBy(Comparator.comparingDouble(x -> x)))))
        .entrySet()
        .parallelStream()
        .map(x -> Map.entry(x.getKey(), x.getValue().orElseThrow()))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public PartitionCost partitionCost(ClusterInfo clusterInfo) {
    var replicaIn = replicaInCount(clusterInfo);
    var dataInRate =
        new TreeMap<TopicPartitionReplica, Double>(
            Comparator.comparing(TopicPartitionReplica::brokerId)
                .thenComparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));
    replicaIn.forEach(
        (tpr, rate) -> {
          var score = rate / brokerBandwidthCap.get(tpr.brokerId());
          if (score >= 1) score = 1;
          dataInRate.put(tpr, score);
        });
    return new PartitionCost() {
      @Override
      public Map<TopicPartition, Double> value(String topic) {
        return dataInRate.entrySet().stream()
            .filter(x -> x.getKey().topic().equals(topic))
            .collect(
                Collectors.groupingBy(
                    x -> new TopicPartition(x.getKey().topic(), x.getKey().partition())))
            .entrySet()
            .stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().stream()
                            .mapToDouble(Map.Entry::getValue)
                            .max()
                            .orElseThrow()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @Override
      public Map<TopicPartition, Double> value(int brokerId) {
        return dataInRate.entrySet().stream()
            .filter(x -> x.getKey().brokerId() == brokerId)
            .collect(
                Collectors.groupingBy(
                    x -> new TopicPartition(x.getKey().topic(), x.getKey().partition())))
            .entrySet()
            .stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().stream()
                            .mapToDouble(Map.Entry::getValue)
                            .max()
                            .orElseThrow()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    };
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Fetcher fetcher() {
    return client -> new java.util.ArrayList<>(KafkaMetrics.TopicPartition.Size.fetch(client));
  }

  public Map<TopicPartitionReplica, Double> replicaInCount(ClusterInfo clusterInfo) {
    Map<TopicPartitionReplica, List<HasBeanObject>> tpBeanObjects = new HashMap<>();
    clusterInfo
        .allBeans()
        .forEach(
            ((broker, beanObjects) ->
                clusterInfo
                    .topics()
                    .forEach(
                        topic ->
                            clusterInfo
                                .replicas(topic)
                                .forEach(
                                    partitionInfo -> {
                                      var beanObject =
                                          beanObjects.stream()
                                              .filter(
                                                  b ->
                                                      b.beanObject()
                                                              .getProperties()
                                                              .get("topic")
                                                              .equals(partitionInfo.topic())
                                                          && Integer.parseInt(
                                                                  b.beanObject()
                                                                      .getProperties()
                                                                      .get("partition"))
                                                              == (partitionInfo.partition()))
                                              .collect(Collectors.toList());
                                      if (!beanObject.isEmpty())
                                        tpBeanObjects.put(
                                            new TopicPartitionReplica(
                                                partitionInfo.topic(),
                                                partitionInfo.partition(),
                                                broker),
                                            beanObject);
                                    }))));
    return tpBeanObjects.entrySet().stream()
        .flatMap(
            tprEntry -> {
              var sortedBeanObjects =
                  tprEntry.getValue().stream()
                      .sorted(
                          Comparator.comparing(
                              HasBeanObject::createdTimestamp, Comparator.reverseOrder()))
                      .collect(Collectors.toList());
              var duration = 2;
              if (sortedBeanObjects.size() < duration) {
                throw new IllegalArgumentException("need more than two metrics to score replicas");
              } else {
                var beanObjectNew = (HasValue) sortedBeanObjects.get(0);
                var beanObjectOld = (HasValue) sortedBeanObjects.get(duration - 1);
                return Map.of(
                    tprEntry.getKey(),
                    (double) (beanObjectNew.value() - beanObjectOld.value())
                        / ((beanObjectNew.createdTimestamp() - beanObjectOld.createdTimestamp())
                            / 1000.0)
                        / 1048576.0)
                    .entrySet()
                    .stream();
              }
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
