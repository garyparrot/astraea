package org.astraea.balancer.alpha.cost;

import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.balancer.alpha.BalancerUtils;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasPartitionCost;
import org.astraea.cost.PartitionCost;
import org.astraea.cost.TopicPartition;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.topic.TopicAdmin;

public class ReplicaMigrateCost implements HasPartitionCost {
  @Override
  public Fetcher fetcher() {
    return client -> new java.util.ArrayList<>(KafkaMetrics.TopicPartition.Size.fetch(client));
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo) {
    var sizeOfReplica = handleBeanObject(clusterInfo);
    var totalUsedSizeInBroker = new HashMap<Integer, Long>();
    TreeMap<TopicPartitionReplica, Double> replicaCost =
        new TreeMap<>(
            Comparator.comparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition)
                .thenComparing(TopicPartitionReplica::brokerId));
    clusterInfo
        .allBeans()
        .keySet()
        .forEach(
            broker -> {
              totalUsedSizeInBroker.put(
                  broker,
                  sizeOfReplica.entrySet().stream()
                      .filter((b) -> b.getKey().brokerId() == broker)
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                      .values()
                      .stream()
                      .mapToLong(Long::longValue)
                      .sum());
            });
    sizeOfReplica.forEach(
        (tpr, size) -> {
          replicaCost.put(tpr, (double) size / totalUsedSizeInBroker.get(tpr.brokerId()));
        });
    return new PartitionCost() {
      @Override
      public Map<TopicPartition, Double> value(String topic) {
        Map<TopicPartition, Double> scores =
            new TreeMap<>(
                Comparator.comparing(TopicPartition::topic)
                    .thenComparing(TopicPartition::partition));
        clusterInfo
            .nodes()
            .forEach(
                nodeInfo -> {
                  replicaCost.entrySet().stream()
                      .filter(
                          (tprScore) ->
                              tprScore.getKey().topic().equals(topic)
                                  && clusterInfo
                                          .partitions(topic)
                                          .get(tprScore.getKey().partition())
                                          .leader()
                                      == nodeInfo)
                      .forEach(
                          tprScore ->
                              scores.put(
                                  TopicPartition.of(
                                      tprScore.getKey().topic(), tprScore.getKey().partition()),
                                  tprScore.getValue()));
                });
        return scores;
      }

      @Override
      public Map<TopicPartition, Double> value(int brokerId) {
        return replicaCost.entrySet().stream()
            .sorted(Comparator.comparing(x -> x.getKey().topic()))
            .collect(Collectors.toList())
            .stream()
            .filter((tprScore) -> tprScore.getKey().brokerId() == brokerId)
            .collect(
                Collectors.toMap(
                    x -> TopicPartition.of(x.getKey().topic(), x.getKey().partition()),
                    Map.Entry::getValue));
      }
    };
  }

  public Map<TopicPartitionReplica, Long> handleBeanObject(ClusterInfo clusterInfo) {
    Map<TopicPartitionReplica, Long> sizeOfReplica = new HashMap<>();
    clusterInfo
        .allBeans()
        .forEach(
            (broker, beanObjects) -> {
              beanObjects.stream()
                  .filter(x -> x instanceof HasValue)
                  .filter(x -> x.beanObject().getProperties().get("type").equals("Log"))
                  .filter(x -> x.beanObject().getProperties().get("name").equals("Size"))
                  .map(x -> (HasValue) x)
                  .forEach(
                      beanObject ->
                          sizeOfReplica.put(
                              new TopicPartitionReplica(
                                  beanObject.beanObject().getProperties().get("topic"),
                                  Integer.parseInt(
                                      beanObject.beanObject().getProperties().get("partition")),
                                  broker),
                              (beanObject).value()));
            });
    return sizeOfReplica;
  }

  public static void main(String[] args) throws InterruptedException, MalformedURLException {
    var host = "localhost";
    var brokerPort = 14179;
    var admin = TopicAdmin.of(host + ":" + brokerPort);
    var allBeans = new HashMap<Integer, Collection<HasBeanObject>>();
    var jmxAddress = Map.of(1001, 11040, 1002, 15006, 1003, 10059);

    ReplicaMigrateCost costFunction = new ReplicaMigrateCost();
    jmxAddress.forEach(
        (b, port) -> {
          var firstBeanObjects =
              BeanCollector.builder()
                  .interval(Duration.ofSeconds(4))
                  .build()
                  .register()
                  .host(host)
                  .port(port)
                  .fetcher(Fetcher.of(Set.of(costFunction.fetcher())))
                  .build()
                  .current();
          allBeans.put(
              b,
              allBeans.containsKey(b)
                  ? Stream.concat(allBeans.get(b).stream(), firstBeanObjects.stream())
                      .collect(Collectors.toList())
                  : firstBeanObjects);
        });
    var clusterInfo = ClusterInfo.of(BalancerUtils.clusterSnapShot(admin), allBeans);
    costFunction
        .partitionCost(clusterInfo)
        .value(1001)
        .forEach((broker, score) -> System.out.println(broker + ":" + score));
    costFunction
        .partitionCost(clusterInfo)
        .value("test-1")
        .forEach(
            (tp, score) -> {
              System.out.println(tp.topic() + " " + tp.partition() + ": " + score);
            });
  }
}
