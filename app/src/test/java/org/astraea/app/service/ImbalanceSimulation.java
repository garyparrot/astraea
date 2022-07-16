package org.astraea.app.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.sun.jna.platform.win32.WinDef;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.NodeInfo;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ImbalanceSimulation extends RequireManyBrokerCluster {

  boolean verbose = false;
  int brokerCount = brokerIds().size();
  int time = 100;
  static Gson gson = new GsonBuilder()
      .registerTypeAdapter(LogPlacement.of(0).getClass(), new Yikes.LogPlacementSerializer())
      .registerTypeAdapter(LogPlacement.class, new Yikes.LogPlacementSerializer())
      .registerTypeHierarchyAdapter(LogPlacement.class, new Yikes.LogPlacementSerializer())
      .registerTypeAdapter(DataSize.class, new Yikes.DataSizeTypeAdapter())
      .setPrettyPrinting()
      .create();

  List<WorkflowPattern> patterns = List.of(
      new AdHocWorkflowPattern(brokerCount),
      new LongRunWorkflowPattern(brokerCount, time));

  Set<String> fuzzyCluster(Admin admin) {
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
        .collect(Collectors.toSet());
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

  /** Apply a very imbalanced simulation */
  Yikes.ClusterSimulation imbalanceSimulation(Map<TopicPartition, List<Replica>> replicas, Set<String> topics) {
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

    // 1
    // Supplier<Double> skewFactor = () ->
    //     Math.pow(ThreadLocalRandom.current().nextDouble(0.75, 1.25), 2);
    Supplier<Double> skewFactor = () ->
        Math.pow(ThreadLocalRandom.current().nextDouble(0.35, 1.25), 2);

    replicas.keySet().forEach(tp -> {
      long partitions = topicPartitionCount.get(tp.topic());
      int fanout = fanoutMap.get(tp.topic());
      double rate = topicProduceRate.get(tp.topic()).divide(partitions).measurement(DataUnit.Byte).doubleValue();
      double skewRate = rate * skewFactor.get();
      DataSize skew = DataUnit.Byte.of((long) skewRate);
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
        .sorted(Comparator.comparing((Map.Entry<String, LongSummaryStatistics> x) -> x.getValue().getMax()))
        .forEach(entry -> {
          var topic = entry.getKey();
          var summary = entry.getValue();
          System.out.printf("[%s] Max: %s, Min: %s, Avg: %s, Imf: %.3f%n",
              topic,
              DataUnit.Byte.of(summary.getMax()),
              DataUnit.Byte.of(summary.getMin()),
              DataUnit.Byte.of((long)summary.getAverage()),
              1 - (double)summary.getMin() / summary.getMax());
        });
    System.out.println();
  }

  void patternContribution(Map<TopicPartition, DataSize> theMap) {
    Map<WorkflowPattern, DataSize> a = theMap.entrySet().stream()
        .map(e -> Map.entry(
            patterns.stream().filter(x -> x.owned(e.getKey().topic())).findFirst().orElseThrow(),
            e.getValue()))
        .collect(Collectors.groupingBy(Map.Entry::getKey,
            Collectors.mapping(Map.Entry::getValue,
                Collectors.reducing(DataUnit.Byte.of(0), DataSize::add))));
    a.forEach((workflow, dataSize) -> {
      System.out.printf("[%s] %s%n", workflow.getClass().getSimpleName(), dataSize);
    });
  }

  void identifyLog(Admin admin, Set<String> topics) {
    Set<NodeInfo> nodes = admin.nodes();
    Map<TopicPartition, List<Replica>> replicas = admin.replicas(topics);

    Map<Integer, String> collect = nodes.stream()
        .collect(Collectors.toMap(
            NodeInfo::id,
            x -> replicas.values().stream()
                .flatMap(Collection::stream)
                .filter(Replica::leader)
                .filter(r -> r.broker() == x.id()).count() + ""));
    System.out.println("Leader Count: " + String.join(", ", collect.values()));

  }

  @RepeatedTest(value = 300)
  void testImbalanceDistribution() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topics = fuzzyCluster(admin);
      var replicas = admin.replicas(topics);
      var simulation = imbalanceSimulation(replicas, topics);

      identifyOutlier(simulation.produceLoading);
      patternContribution(simulation.produceLoading);
      identifyLog(admin, Set.copyOf(topics));

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

      admin.deleteTopics(Set.copyOf(topics));
      Utils.packException(()->TimeUnit.SECONDS.sleep(10));
    }
  }
  @Test
  void testBandwidthInfluence() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      var count = 1_000_000;
      var resultFile = Path.of("/home/garyparrot/bandwidth");
      System.out.println("Generating cluster");
      var topics = fuzzyCluster(admin);
      System.out.println("Done");
      var nodes = admin.nodes();
      var replicas = admin.replicas(topics);
      var executorService = Executors.newCachedThreadPool();
      var dir = Path.of("/home/garyparrot/clusters");
      var allocation = LayeredClusterLogAllocation.of(admin.clusterInfo());
      var drawer = new ExperimentDrawer();
      if(Files.exists(dir))
        Utils.packException(() -> {
          Files.walk(dir).forEach(i -> {
            System.out.println("Delete " + i);
            if(Files.isRegularFile(i))
              Utils.packException(() -> Files.delete(i));
          });
          Files.delete(dir);
        });
      Utils.packException(() -> Files.createDirectories(dir));

      Set<Integer> nodes2 = nodes.stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet());
      var discovered = (Consumer<Yikes.ClusterSimulation>) (simulation) -> {
        var imbalanceFactor = imbalanceFactor(nodes2, simulation);
        var file = Path.of("/home/garyparrot/clusters/cluster_" + (int)(imbalanceFactor * 100) + "%");
        storeSimulation(file, allocation, simulation, false);
      };

      if(!Files.exists(resultFile))
        Utils.packException(() -> Files.createFile(resultFile));

      Stream.generate(() -> imbalanceSimulation(replicas, topics))
          .parallel()
          .limit(count)
          .peek(s -> drawer.add((int)(imbalanceFactor(nodes2, s) * 100)))
          .forEach(discovered);
      executorService.shutdownNow();
    }
  }

  @Test
  void applyPlan() {
    Path path = Path.of("/home/garyparrot/clusters/cluster_39%");
    new Yikes().applyCluster(path.toString());

    var bootstrap = "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655";
    try (Admin admin = Admin.of(bootstrap)) {
      for(int i = 0; i < 4;i++) {
        var configResource = new ConfigResource(ConfigResource.Type.BROKER, "" + i);
        var configEntry = new ConfigEntry("log.retention.bytes", Long.toString(DataUnit.GB.of(5).measurement(DataUnit.Byte).longValue()));
        var alterOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        admin.config(Map.of(configResource, List.of(alterOp)));
      }
    }
  }

  @Test
  void shit() {
    var bootstrap = "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655";
    try (Admin admin = Admin.of(bootstrap)) {
      admin.topicNames().stream()
          .filter(x -> !x.startsWith("_"))
          .forEach(topic -> {
            System.out.println(topic);
            var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            var configEntry = new ConfigEntry("retention.bytes", Long.toString(DataUnit.MiB.of(512).measurement(DataUnit.Byte).longValue()));
            var alterOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
            admin.config(Map.of(configResource, List.of(alterOp)));
          });
    }
  }

  @Test
  void aaa() throws IOException {
    Path path = Path.of("/home/garyparrot/clusters/cluster_39%");
    describePlan(path.toString());

    var allocation = recoverAllocation(path);
    var produce = recoverProduce(path);

    var map = new HashMap<Integer, DataSize>();

    allocation.forEach((tp, logs) -> {
      var leader = logs.get(0).broker();
      var followers = logs.subList(1, logs.size()).stream()
          .map(LogPlacement::broker)
          .collect(Collectors.toUnmodifiableList());
      followers.forEach(id -> {
        map.computeIfAbsent(id, (i) -> DataUnit.Byte.of(0));
        map.put(id, map.get(id).add(produce.get(tp)));
      });
    });

    map.forEach((broker, rate) -> {
      System.out.printf("[broker %d]: %s%n", broker, rate);
    });
  }

  @Test
  void aaa2() throws IOException {
    Path path = Path.of("/home/garyparrot/clusters/cluster_39%");
    describePlan(path.toString());

    var allocation = recoverAllocation(path);
    var produce = recoverProduce(path);

    var map = new HashMap<Integer, DataSize>();

    allocation.forEach((tp, logs) -> {
      var leader = logs.get(0).broker();
      map.computeIfAbsent(leader, (i) -> DataUnit.Byte.of(0));
      map.put(leader, map.get(leader).add(produce.get(tp)));
    });

    map.forEach((broker, rate) -> {
      System.out.printf("[broker %d]: %s%n", broker, rate);
    });
  }

  @Test
  void generateLoadFiles() throws IOException {
    Path path = Path.of("/home/garyparrot/clusters/cluster_39%");
    describePlan(path.toString());
    Path produceLoading = Path.of("/home/garyparrot/Programming/ansible/producer-inventory.json");
    Path consumeLoading = Path.of("/home/garyparrot/Programming/ansible/consumer-inventory.json");

    // var produce = recoverProduce(path).entrySet().stream()
    //     .collect(Collectors.groupingBy(x -> x.getKey().topic(),
    //         Collectors.mapping(Map.Entry::getValue,
    //             Collectors.reducing(DataUnit.Byte.of(0), DataSize::add))));
    // var consume = recoverConsume(path).entrySet().stream()
    //     .collect(Collectors.groupingBy(x -> x.getKey().topic(),
    //         Collectors.mapping(Map.Entry::getValue,
    //             Collectors.reducing(DataUnit.Byte.of(0), DataSize::add))));
    var hosts = Map.of(
        "192.168.103.181", DataUnit.Gib.of(10),
        "192.168.103.182", DataUnit.Gib.of(10));
    Yikes.writeAnsibleLoadingByTp(produceLoading, "Producer", recoverProduce(path), hosts);
    // Yikes.writeAnsibleLoading(consumeLoading, "Consumer", consume, hosts);
  }

  @Test
  void WAT(){
    Path path = Path.of("/home/garyparrot/clusters/cluster_39%");
    Path produceLoading = Path.of("/tmp/aaa" + Utils.randomString());
    var hosts = Map.of(
        "192.168.103.181", DataUnit.Gib.of(10),
        "192.168.103.182", DataUnit.Gib.of(10));
    Yikes.writeAnsibleLoadingByTp(produceLoading, "Producer", recoverProduce(path), hosts);

  }

  static Map<TopicPartition, List<LogPlacement>> recoverAllocation(Path path) {
    Type type0 = new TypeToken<Map<String, List<LogPlacement>>>() {}.getType();
    Type type1 = new TypeToken<Map<String, DataSize>>() {}.getType();
    try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
      JsonObject object = gson.fromJson(bufferedReader, JsonObject.class);
      return  ((Map<String, List<LogPlacement>>) gson.fromJson(object.get("allocation"), type0))
          .entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(
              x -> TopicPartition.of(x.getKey()),
              Map.Entry::getValue));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
  static Map<TopicPartition, DataSize> recoverProduce(Path path) {
    Type type0 = new TypeToken<Map<String, List<LogPlacement>>>() {}.getType();
    Type type1 = new TypeToken<Map<String, DataSize>>() {}.getType();
    try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
      JsonObject object = gson.fromJson(bufferedReader, JsonObject.class);
      return ((Map<String, DataSize>)gson.fromJson(object.get("produce"), type1))
          .entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(
              x -> TopicPartition.of(x.getKey()),
              Map.Entry::getValue));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
  static Map<TopicPartition, DataSize> recoverConsume(Path path) {
    Type type0 = new TypeToken<Map<String, List<LogPlacement>>>() {}.getType();
    Type type1 = new TypeToken<Map<String, DataSize>>() {}.getType();
    try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
      JsonObject object = gson.fromJson(bufferedReader, JsonObject.class);
      return ((Map<String, DataSize>)gson.fromJson(object.get("consume"), type1))
          .entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(
              x -> TopicPartition.of(x.getKey()),
              Map.Entry::getValue));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = "/home/garyparrot/clusters/cluster_39%")
  void describePlan(String p) throws IOException {
    Path path = Path.of(p);

    var allocation = recoverAllocation(path);
    var produce = recoverProduce(path);
    var consume = recoverConsume(path);

    System.out.println("Produce contribution");
    patternContribution(produce);

    Map<Integer, Long> aggregatedBrokerIngress = allocation.entrySet().stream()
        .flatMap(entry -> entry.getValue().stream().map(x -> Map.entry(x.broker(), entry.getKey())))
        .map(entry -> Map.entry(entry.getKey(), produce.get(entry.getValue())))
        .collect(Collectors.groupingBy(Map.Entry::getKey,
            Collectors.mapping(x -> x.getValue().measurement(DataUnit.Byte).longValue(),
                Collectors.reducing(0L, Long::sum))));
    Map<Integer, Long> aggregatedBrokerEgress = allocation.entrySet().stream()
        .flatMap(entry -> entry.getValue().stream().map(x -> Map.entry(x.broker(), entry.getKey())))
        .map(entry -> Map.entry(entry.getKey(), consume.get(entry.getValue())))
        .collect(Collectors.groupingBy(Map.Entry::getKey,
            Collectors.mapping(x -> x.getValue().measurement(DataUnit.Byte).longValue(),
                Collectors.reducing(0L, Long::sum))));


    aggregatedBrokerIngress.keySet().stream()
        .sorted()
        .forEach(node -> {
          System.out.printf("[Node %d] Produce: %s, Consume: %s%n", node,
              DataUnit.Byte.of(aggregatedBrokerIngress.get(node)),
              DataUnit.Byte.of(aggregatedBrokerEgress.get(node)));
        });
    System.out.println(imbalanceFactor(aggregatedBrokerIngress));
  }

  static void storeSimulation(Path file, ClusterLogAllocation allocation, Yikes.ClusterSimulation simulation, boolean overwrite) {
    Type type = new TypeToken<Map<TopicPartition, List<LogPlacement>>>() {}.getType();
    var collect = allocation.topicPartitionStream()
        .collect(Collectors.toUnmodifiableMap(
            x -> x,
            allocation::logPlacements));
    JsonElement jsonAllocation = gson.toJsonTree(collect, type);
    JsonElement jsonProduce = gson.toJsonTree(simulation.produceLoading);
    JsonElement jsonConsume = gson.toJsonTree(simulation.consumeLoading);

    JsonObject jsonObject = new JsonObject();
    jsonObject.add("allocation", jsonAllocation);
    jsonObject.add("produce", jsonProduce);
    jsonObject.add("consume", jsonConsume);

    Utils.packException(() -> {
      if(Files.exists(file) && !overwrite)
        return;
      Files.deleteIfExists(file);
      System.out.println("Write file " + file);
      BufferedWriter bufferedWriter = Files.newBufferedWriter(file);
      bufferedWriter.write(jsonObject.toString());
      bufferedWriter.close();
    });
  }

  static double imbalanceFactor(Map<Integer, Long> rate) {
    LongSummaryStatistics summary = rate.values().stream()
        .mapToLong(x -> x)
        .summaryStatistics();

    return 1.0 - (double)summary.getMin() / summary.getMax();
  }

  static double imbalanceFactor(Set<Integer> nodes, Yikes.ClusterSimulation simulation) {
    LongSummaryStatistics summary = nodes.stream()
        .collect(Collectors.toMap(
            x -> x,
            simulation::calculateBrokerEgress))
        .values()
        .stream()
        .mapToLong(x -> x.toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS).longValueExact())
        .summaryStatistics();

    return 1.0 - (double)summary.getMin() / summary.getMax();
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
    return ThreadLocalRandom.current().nextInt(5, 15);
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
    DataSize max = DataUnit.KiB.of(200).multiply(nodes);
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
    return ThreadLocalRandom.current().nextInt(3, 5);
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
    this.topicCreation =  new BinomialDistribution(1, 10.0 / maxTime);
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
    long minValue = DataUnit.MiB.of(20).measurement(DataUnit.Byte).longValue();
    long maxValue = DataUnit.MiB.of(50).measurement(DataUnit.Byte).longValue();
    return DataUnit.Byte.of((long)(minValue + maxValue * normal.sample()));
    // return DataUnit.Byte.of(ThreadLocalRandom.current().nextLong(minValue, maxValue));
  }

  @Override
  public int replicaFactor() {
    double d = ThreadLocalRandom.current().nextDouble();
    if(d < 0.7) return 1;
    return 2;
  }

  @Override
  public int consumerFanout() {
    return ThreadLocalRandom.current().nextInt(1, 3);
  }
}

class ExperimentDrawer {
  final Map<Integer, Long> experiments;
  private long next;

  ExperimentDrawer() {
    experiments = new HashMap<>();
    next = System.currentTimeMillis();
  }

  void add(int key) {
    experiments.putIfAbsent(key, 0L);
    experiments.put(key, experiments.get(key) + 1);
    if(System.currentTimeMillis() > next)
      isTimeToDraw();
  }

  void isTimeToDraw() {
    next = System.currentTimeMillis() + Duration.ofSeconds(1).toMillis();
    Yikes.virtualize(experiments);
    System.out.println();
  }
}