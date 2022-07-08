package org.astraea.app.service;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;
import org.junit.jupiter.api.RepeatedTest;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ImbalanceSimulation extends RequireManyBrokerCluster {

  boolean verbose = false;
  int brokerCount = brokerIds().size();
  int time = 100;

  List<WorkflowPattern> patterns = List.of(
      new AdHocWorkflowPattern(brokerCount),
      new LongRunWorkflowPattern(brokerCount, time));

  List<String> fuzzyCluster(Admin admin) {
    Map<Integer, List<String>> toDelete = new HashMap<>();
    for(int i = 0;i < time; i++) {
      // list of pattern creation
      List<Runnable> creation = new ArrayList<>();

      // pattern creation
      final int timeNow = i;
      patterns.forEach(patterns -> {
        int cnt = patterns.nextTopicCreationCount();
        IntStream.range(0, cnt)
            .forEach(j -> creation.add(() -> {
              String topicName = patterns.topicName();
              int partitions = patterns.nextTopicPartitionSize();
              short replicas = (short) patterns.replicaFactor();
              int life = patterns.nextTopicLifeLength() + timeNow;
              if(verbose)
                System.out.printf("[%s] create %s with %d partition and %d replicas (life %d).%n",
                    patterns.getClass().getSimpleName(), topicName, partitions, replicas, life);

              // create
              admin.creator()
                  .topic(topicName)
                  .numberOfPartitions(partitions)
                  .numberOfReplicas(replicas)
                  .create();

              // mark deletion
              toDelete.putIfAbsent(life, new ArrayList<>());
              toDelete.get(life).add(topicName);
            }));
      });

      // topic to remove
      toDelete.getOrDefault(timeNow, List.of()).forEach(topicToDelete -> {
        creation.add(() -> {
          if(verbose)
            System.out.println("[CleanUp] Delete topic " + topicToDelete);
          admin.deleteTopics(Set.of(topicToDelete));
        });
      });
      toDelete.computeIfPresent(timeNow, (k, v) -> {
        v.clear();
        return v;
      });

      // shuffle the creation and then execute
      creation.sort(Comparator.comparing(Object::hashCode));
      creation.forEach(Runnable::run);
    }

    // return the remaining topics
    return toDelete.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  /** Apply a very balanced simulation, probably no outlier in there */
  Yikes.ClusterSimulation simulation(Admin admin, List<String> topics) {
    var replicas = admin.replicas(Set.copyOf(topics));
    var simulation = new Yikes.ClusterSimulation(replicas);

    Map<String, Long> topicPartitionCount = replicas.keySet().stream()
        .map(TopicPartition::topic)
        .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
    Map<String, DataSize> topicProduceRate = topics.stream()
        .collect(Collectors.toMap(
            topic -> topic,
            topic -> patterns.stream()
                .filter(p -> p.owned(topic))
                .findFirst()
                .orElseThrow()
                .nextProduceRate()));
    Map<String, Integer> fanoutMap = topics.stream()
        .collect(Collectors.toMap(
            topic -> topic,
            topic -> patterns.stream()
                .filter(p -> p.owned(topic))
                .findFirst()
                .orElseThrow()
                .consumerFanout()));

    replicas.keySet().forEach(tp -> {
      long partitions = topicPartitionCount.get(tp.topic());
      int fanout = fanoutMap.get(tp.topic());
      double rate = topicProduceRate.get(tp.topic()).divide(partitions).measurement(DataUnit.Byte).doubleValue();
      DataSize skew = DataUnit.Byte.of((long) rate);
      simulation.applyProducerLoading(tp, skew);
      simulation.applyConsumerLoading(tp, skew, fanout);
    });

    return simulation;
  }

  void identifyOutlier(Map<TopicPartition, DataSize> theMap) {
    Map<String, List<DataSize>> topicLoadings = theMap.entrySet().stream()
        .collect(Collectors.groupingBy(x -> x.getKey().topic(),
            Collectors.mapping(Map.Entry::getValue,
                Collectors.toUnmodifiableList())));
    Map<String, LongSummaryStatistics> loadingSummary = topicLoadings.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().stream()
                .mapToLong(x -> x.measurement(DataUnit.Byte).longValue())
                .summaryStatistics()));
    System.out.println("Print Outlier");
    loadingSummary.entrySet()
        .stream()
        .filter(x -> x.getValue().getMin() != x.getValue().getMax())
        .sorted(Comparator.comparing((Map.Entry<String, LongSummaryStatistics> x) -> (double)x.getValue().getMin() / x.getValue().getMax()).reversed())
        .forEach(entry -> {
          var topic = entry.getKey();
          var summary = entry.getValue();
          System.out.printf("[%s] Max: %d, Min: %d, Avg: %.3f, Imf: %.3f%n",
              topic,
              summary.getMax(),
              summary.getMin(),
              summary.getAverage(),
              1 - (double)summary.getMin() / summary.getMax());
        });
    System.out.println();
  }

  @RepeatedTest(value = 300)
  void testImbalanceDistribution() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topics = fuzzyCluster(admin);
      var simulation = simulation(admin, topics);

      simulation.produceLoading.entrySet()
          .stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach((entry) -> {
            System.out.println(entry.getKey() + ": " + entry.getValue());
          });

      System.out.println("Remaining topics: " + topics.size());
      brokerIds().stream()
          .sorted()
          .forEach(node -> {
            System.out.printf("[Node %d] Produce: %s, Consume: %s%n", node,
                simulation.calculateBrokerIngress(node),
                simulation.calculateBrokerEgress(node));
          });
      var summary = brokerIds().stream()
          .map(simulation::calculateBrokerIngress)
          .map(x -> x.toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS))
          .mapToDouble(BigDecimal::doubleValue)
          .summaryStatistics();

      var imbalanceFactor = 1 - summary.getMin() / summary.getMax();
      var imbalanceFactorString = String.format("%.6f%n", imbalanceFactor);
      System.out.println("Imbalance factor: " + imbalanceFactor);

      Path store = Path.of("/home/garyparrot/imbalance-factors");
      if(!Files.exists(store))
        Utils.packException(() -> Files.createFile(store));
      Utils.packException(() -> Files.writeString(store, imbalanceFactorString, StandardOpenOption.APPEND));

      identifyOutlier(simulation.produceLoading);

      admin.deleteTopics(Set.copyOf(topics));
      Utils.packException(()->TimeUnit.SECONDS.sleep(10));
    }
  }

  /** Describe a kind of kafka usage workload */
  interface WorkflowPattern {
    String topicName();
    boolean owned(String topicName);
    int nextTopicCreationCount();
    int nextTopicLifeLength();
    int nextTopicPartitionSize();
    DataSize nextProduceRate();
    int replicaFactor();
    int consumerFanout();
  }

}

class AdHocWorkflowPattern implements ImbalanceSimulation.WorkflowPattern {

  private final int nodes;
  private final RealDistribution normal = new LogNormalDistribution(0, 1);

  public AdHocWorkflowPattern(int nodes) {
    this.nodes = nodes;
  }

  public String topicName() {
    return "AdHoc-" + Utils.randomString();
  }

  @Override
  public boolean owned(String topicName) {
    return topicName.indexOf("AdHoc-") == 0;
  }

  @Override
  public int nextTopicCreationCount() {
    return ThreadLocalRandom.current().nextInt(5, 10);
  }

  @Override
  public int nextTopicLifeLength() {
    return (int)(Math.abs(ThreadLocalRandom.current().nextGaussian() * 10)) + 1;
  }

  @Override
  public int nextTopicPartitionSize() {
    return ThreadLocalRandom.current().nextInt(3, 10);
    // double v = Math.abs(ThreadLocalRandom.current().nextGaussian());
    // return (int)(v * (10 - nodes)) + nodes;
  }

  @Override
  public DataSize nextProduceRate() {
    DataSize max = DataUnit.MiB.of(1).multiply(nodes);
    // long next = ThreadLocalRandom.current().nextLong(0L, max.measurement(DataUnit.Byte).longValue());
    long next = (long)((5*normal.sample()) * max.measurement(DataUnit.Byte).longValue());
    return DataUnit.Byte.of(next);
  }

  @Override
  public int replicaFactor() {
    double v = ThreadLocalRandom.current().nextDouble();
    if(v < 0.7) return 1;
    if(v < 0.9) return 2;
    return 3;
  }

  @Override
  public int consumerFanout() {
    return ThreadLocalRandom.current().nextInt(1, 30);
  }
}

class LongRunWorkflowPattern implements ImbalanceSimulation.WorkflowPattern {

  private final int maxTime;
  private final int nodes;
  private final IntegerDistribution topicCreation ;
  private final RealDistribution partitionSize = new NormalDistribution();
  private final RealDistribution normal = new LogNormalDistribution(0, 1);
  public LongRunWorkflowPattern(int nodes, int maxTime) {
    this.nodes = nodes;
    this.maxTime = maxTime;
    this.topicCreation =  new BinomialDistribution(1, 50.0 / maxTime);
  }

  @Override
  public String topicName() {
    return "LongRun-" + Utils.randomString();
  }

  @Override
  public boolean owned(String topicName) {
    return topicName.indexOf("LongRun-") == 0;
  }

  @Override
  public int nextTopicCreationCount() {
    return topicCreation.sample();
  }

  @Override
  public int nextTopicLifeLength() {
    return maxTime * 2;
  }

  @Override
  public int nextTopicPartitionSize() {
    double v = Math.abs(ThreadLocalRandom.current().nextGaussian());
    return nodes * (int)(3 * (v+1));
  }

  @Override
  public DataSize nextProduceRate() {
    long minValue = DataUnit.MiB.of(10).measurement(DataUnit.Byte).longValue();
    long maxValue = DataUnit.MiB.of(300).measurement(DataUnit.Byte).longValue();
    return DataUnit.Byte.of((long)(minValue + maxValue / 2.0 * normal.sample()));
    // return DataUnit.Byte.of(ThreadLocalRandom.current().nextLong(minValue, maxValue));
  }

  @Override
  public int replicaFactor() {
    return ThreadLocalRandom.current().nextInt(1, 4);
  }

  @Override
  public int consumerFanout() {
    return ThreadLocalRandom.current().nextInt(1, 3);
  }
}