/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.sun.jna.platform.unix.X11;
import kafka.utils.Json;
import net.bytebuddy.description.method.MethodDescription;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.NodeInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Int;

import javax.xml.crypto.Data;

class Yikes extends RequireManyBrokerCluster {

  boolean verbose = true;


  // for topic creation, use uniform distribution.
  double partitionCreationRate = 0.7;
  Supplier<Boolean> shouldCreate =
      () -> ThreadLocalRandom.current().nextDouble(0, 1) <= partitionCreationRate;

  // for creation size, use beta(0.5, 0.5) distribution
  NormalDistribution topicCreationSize = new NormalDistribution(20, 10);
  int partitionCreationSizeAverage = 1; // (int) (10 / topicCreationSize.getNumericalMean());
  Supplier<Integer> nextSize =
      () -> Math.max((int) (topicCreationSize.sample() * partitionCreationSizeAverage), 3);

  // for creation replica factor, use uniform distribution
  Supplier<Short> nextReplicaFactor = () -> (short) ThreadLocalRandom.current().nextInt(1, 4);

  // for topic life
  BetaDistribution betaDistribution1 = new BetaDistribution(0.2, 0.5);
  BinomialDistribution betaDistribution1_2 = new BinomialDistribution(120, 0.2);
  BinomialDistribution betaDistribution1_3 = new BinomialDistribution(120, 0.8);

  int partitionLifeAverage = (int) (80 / betaDistribution1_2.getNumericalMean());
  Supplier<Integer> nea = () -> ThreadLocalRandom.current().nextDouble() < 0.5 ? betaDistribution1_3.sample() : betaDistribution1_2.sample();
  Supplier<Integer> nextLife =
      () -> Math.max(nea.get(), 5);

  // for produce & consume size
  BetaDistribution produceRateDistribution = new BetaDistribution(0.1, 0.3);
  BetaDistribution consumeRateDistribution = new BetaDistribution(0.1, 0.3);
  BinomialDistribution aaa = new BinomialDistribution(100000, 0.002);
  BinomialDistribution bbb = new BinomialDistribution(200, 0.5);
  double produceRateMeanKB = 30 * 1e3 / bbb.getNumericalMean();
  double consumeRateMeanKB = 30 * 1e3 / produceRateDistribution.getNumericalMean();
  Supplier<DataSize> nextProduceRate =
      () -> DataUnit.MB.of(ThreadLocalRandom.current().nextDouble() < 0.9 ? (aaa.sample()) : (bbb.sample()));
  Supplier<DataSize> nextConsumeRate =
      () -> DataUnit.KB.of((long) (consumeRateMeanKB * consumeRateDistribution.sample()));

  Map<Number, Long> experimentMap(Stream<? extends Number> numbers, int maxSize) {
    return numbers.limit(maxSize)
        .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
  }

  static Gson gson = new GsonBuilder()
      .registerTypeAdapter(LogPlacement.of(0).getClass(), new LogPlacementSerializer())
      .registerTypeAdapter(LogPlacement.class, new LogPlacementSerializer())
      .registerTypeHierarchyAdapter(LogPlacement.class, new LogPlacementSerializer())
      .registerTypeAdapter(DataSize.class, new DataSizeTypeAdapter())
      .disableHtmlEscaping()
      .setPrettyPrinting()
      .create();

  @Test
  @DisplayName("See the distribution in use")
  void visualizeDistribution() {
    System.out.println("Creation size");
    virtualize(experimentMap(Stream.generate(nextSize), 10000));
    System.out.println();

    System.out.println("Replica Factor");
    virtualize(experimentMap(Stream.generate(nextReplicaFactor), 10000));
    System.out.println();

    System.out.println("Topic Life Span");
    virtualize(experimentMap(Stream.generate(nextLife), 10000));
    System.out.println();

    System.out.println("Topic Produce Rate");
    virtualize(experimentMap(Stream.generate(() ->
        nextProduceRate.get().measurement(DataUnit.MB).longValue()), 10000));
    System.out.println();

    // System.out.println("Topic Consumer Rate");
    // virtualize(experimentMap(Stream.generate(() ->
    //     nextConsumeRate.get().measurement(DataUnit.MB).longValue() / 10), 10000));
    // System.out.println();

    System.out.println("Consume Fanout Rate");
    virtualize(experimentMap(Stream.generate(() ->
        nextReplicaFactor.get()), 10000));
    System.out.println();
  }

  void distributionVisualizer(int trials, Supplier<Number> outcome) {
    // do experiment & collect outcome
    var valueCountingMap = new HashMap<Double, Integer>();
    IntStream.range(0, trials)
        .forEach(
            i -> {
              double outcomeValue = outcome.get().doubleValue();
              int now = valueCountingMap.getOrDefault(outcomeValue, 0);
              valueCountingMap.put(outcomeValue, now + 1);
            });

    // draw the diagram
    var steps = 10;
    var summary = valueCountingMap.keySet().stream().mapToDouble(x -> x).summaryStatistics();
    var max = summary.getMax();
    var min = summary.getMin();
    var stepSize = (max - min) / (steps);
    Map<Integer, Long> aggregated =
        valueCountingMap.entrySet().stream()
            .map(
                e -> {
                  int newKey = (int) (e.getKey() / stepSize);
                  return Map.entry(newKey, e.getValue());
                })
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey, Collectors.summingLong(Map.Entry::getValue)));
    var barCount = 50;
    var valueSummary = aggregated.values().stream().mapToLong(x -> x).summaryStatistics();
    var barSize = Math.max(valueSummary.getMax() / barCount, 1);

    System.out.println("Max: " + max);
    System.out.println("Min: " + min);
    System.out.println("Mean: " + summary.getAverage());
    aggregated
        .keySet()
        .forEach(
            tile -> {
              long count = aggregated.get(tile);
              int theBars = (int) (count / barSize);
              var barChars =
                  String.join("", Collections.nCopies(theBars, "|"))
                      + String.join("", Collections.nCopies(Math.max(barCount - theBars, 0), " "));
              System.out.printf("[%6d] [%s] [%d trials]%n", tile, barChars, count);
            });
  }

  static void virtualize(Map<? extends Number, Long> map) {
    var barCount = 50;
    var valueSummary = map.values().stream().mapToLong(x -> x).summaryStatistics();
    var barSize = (valueSummary.getMax() / barCount) + 1;

    map.keySet().stream()
        .sorted()
        .forEach(
            tile -> {
              long count = map.get(tile);
              int theBars = barSize <= 0 ? 0 : (int) (count / barSize);
              var barChars =
                  String.join("", Collections.nCopies(theBars, "|"))
                      + String.join("", Collections.nCopies(Math.max(barCount - theBars, 0), " "));
              System.out.printf(
                  "[%6.3f] [%s] [%d trials, %.2f%%]%n",
                  tile.doubleValue(),
                  barChars,
                  count,
                  (double) count * 100.0 / valueSummary.getSum());
            });
  }

  void virtualizeCumulative(Map<? extends Number, Long> map) {
    var barCount = 50;
    var valueSummary = map.values().stream().mapToLong(x -> x).summaryStatistics();
    var barSize = (valueSummary.getSum() / barCount) + 1;

    AtomicLong acc = new AtomicLong();
    map.keySet().stream()
        .sorted()
        .forEach(
            tile -> {
              long count = map.get(tile);
              count = acc.accumulateAndGet(count, Long::sum);
              int theBars = barSize <= 0 ? 0 : (int) (count / barSize);
              var barChars =
                  String.join("", Collections.nCopies(theBars, "|"))
                      + String.join("", Collections.nCopies(Math.max(barCount - theBars, 0), " "));
              System.out.printf(
                  "[%6.3f] [%s] [%d trials, %.2f%%]%n",
                  tile.doubleValue(),
                  barChars,
                  count,
                  (double) count * 100.0 / valueSummary.getSum());
            });
  }

  static List<TopicOperation> operations = new ArrayList<>();

  Set<String> fuzzyCluster(Admin admin, int simulationTime) {
    operations.clear();

    var deathNoteBook = new HashMap<String, Integer>();

    for (int time = 0; time < simulationTime; time++) {
      // System.out.println("[Iteration #" + time + "]");
      // do creation
      if (shouldCreate.get()) {
        String topicName = Utils.randomString();
        int topicLife = nextLife.get();
        short replicas = nextReplicaFactor.get();
        int partitionSize = nextSize.get();

        if (verbose) {
          System.out.printf(
              "Create topic '%s' with partition size %d & replica size %d, life cycle %d%n",
              topicName, partitionSize, replicas, topicLife);
        }
        admin
            .creator()
            .topic(topicName)
            .numberOfPartitions(partitionSize)
            .numberOfReplicas(replicas)
            .create();
        operations.add(new TopicOperation(
            TopicOperation.Operation.CREATE,
            topicName,
            partitionSize,
            replicas));

        deathNoteBook.put(topicName, topicLife + time);
      }

      // kill topics
      final var currentTime = time;
      final var toKill =
          deathNoteBook.entrySet().stream()
              .filter(x -> x.getValue() < currentTime)
              .map(Map.Entry::getKey)
              .collect(Collectors.toUnmodifiableSet());
      if (!toKill.isEmpty()) Utils.packException(() -> TimeUnit.MILLISECONDS.sleep(100));
      toKill.forEach(name -> operations.add(new TopicOperation(
          TopicOperation.Operation.DELETE,
          name,
          0,
          0)));
      admin.deleteTopics(toKill);
      toKill.forEach(deathNoteBook::remove);
    }
    return deathNoteBook.keySet();
  }

  // specify loading for topic/partition
  Map<Integer, BrokerNetworkLoad> simulateLoading(Admin admin, Set<String> topics) {
    final var replicas = admin.replicas(topics);

    // simulate the loading
    ClusterSimulation simulation = new ClusterSimulation(replicas);
    BetaDistribution produceRateDistribution = new BetaDistribution(0.1, 0.3);
    BetaDistribution consumeRateDistribution = new BetaDistribution(0.1, 0.3);
    double produceRateMeanKB = 8 * 1e3 / produceRateDistribution.getNumericalMean();
    double consumeRateMeanKB = 8 * 1e3 / produceRateDistribution.getNumericalMean();
    Supplier<DataSize> nextProduceRate =
        () -> DataUnit.KB.of((long) (produceRateMeanKB * produceRateDistribution.sample()));
    Supplier<DataSize> nextConsumeRate =
        () -> DataUnit.KB.of((long) (consumeRateMeanKB * consumeRateDistribution.sample()));
    Supplier<Integer> nextFanout = () -> ThreadLocalRandom.current().nextInt(1, 3);
    replicas
        .keySet()
        .forEach(
            topicPartition -> {
              var produceRate = nextProduceRate.get();
              simulation.applyProducerLoading(topicPartition, produceRate);
              simulation.applyConsumerLoading(topicPartition, produceRate, nextFanout.get());
            });

    Set<NodeInfo> nodes = admin.nodes();

    return nodes.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                NodeInfo::id,
                node ->
                    new BrokerNetworkLoad(
                        simulation.calculateBrokerIngress(node.id()),
                        simulation.calculateBrokerEgress(node.id()))));
  }

  private static ClusterSimulation theSimulation;

  // specify loading for topic, spread evenly on each partitions
  Map<Integer, BrokerNetworkLoad> simulateLoading2(Admin admin, Set<String> topics) {
    final var replicas = admin.replicas(topics);

    // simulate the loading
    ClusterSimulation simulation = new ClusterSimulation(replicas);
    theSimulation = simulation;
    Supplier<Integer> nextFanout = () -> ThreadLocalRandom.current().nextInt(1, 3);
    Map<String, DataSize> topicProduceRate =
        topics.stream().collect(Collectors.toUnmodifiableMap(x -> x, x -> nextProduceRate.get()));
    Map<String, DataSize> topicConsumeRate =
        topics.stream().collect(Collectors.toUnmodifiableMap(x -> x, x -> nextConsumeRate.get()));
    Map<String, Integer> topicConsumeFanout =
        topics.stream().collect(Collectors.toUnmodifiableMap(x -> x, x -> nextFanout.get()));
    Function<String, Long> topicSize =
        (theTopic) -> replicas.keySet().stream().filter(x -> x.topic().equals(theTopic)).count();
    replicas
        .keySet()
        .forEach(
            topicPartition -> {
              var topicName = topicPartition.topic();
              var produce = topicProduceRate.get(topicName).divide(topicSize.apply(topicName));
              var consume = topicConsumeRate.get(topicName).divide(topicSize.apply(topicName));
              var consumeFanout = topicConsumeFanout.get(topicName);
              simulation.applyProducerLoading(topicPartition, produce);
              simulation.applyConsumerLoading(topicPartition, produce, consumeFanout);
            });

    Set<NodeInfo> nodes = admin.nodes();

    return nodes.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                NodeInfo::id,
                node ->
                    new BrokerNetworkLoad(
                        simulation.calculateBrokerIngress(node.id()),
                        simulation.calculateBrokerEgress(node.id()))));
  }

  Stream<Map<Integer, BrokerNetworkLoad>> repeatSimulation(Admin admin, int simulateTime) {
    return Stream.generate(
        () -> {
          Set<String> topics = fuzzyCluster(admin, simulateTime);
          Map<Integer, BrokerNetworkLoad> integerBrokerNetworkLoadMap =
              simulateLoading2(admin, topics);
          Utils.packException(() -> TimeUnit.MILLISECONDS.sleep(200));
          admin.deleteTopics(topics);
          return integerBrokerNetworkLoadMap;
        });
  }

  @RepeatedTest(value = 30)
  void test() throws IOException {
    int simulationTime = 100;
    Path history = Path.of("/home/garyparrot/numbers");
    if (!history.toFile().exists()) Files.createFile(history);
    try (Admin admin = Admin.of(bootstrapServers())) {
      AtomicInteger counter = new AtomicInteger();
      Stream<Double> stream =
          repeatSimulation(admin, simulationTime)
              .map(
                  map -> {
                    var maxIngress =
                        map.values().stream()
                            .mapToDouble(
                                x ->
                                    x.ingress
                                        .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS)
                                        .doubleValue())
                            .max()
                            .orElseThrow();
                    var minIngress =
                        map.values().stream()
                            .mapToDouble(
                                x ->
                                    x.ingress
                                        .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS)
                                        .doubleValue())
                            .min()
                            .orElseThrow();
                    System.out.println("Min: " + DataUnit.Byte.of((long) minIngress));
                    System.out.println("Max: " + DataUnit.Byte.of((long) maxIngress));
                    return (double) ((maxIngress - minIngress) / maxIngress);
                  })
              .peek(x -> counter.incrementAndGet())
              .peek(
                  diffRatio ->
                      System.out.printf("(iteration %d) Diff: %.3f%n", counter.get(), diffRatio));

      stream.forEach(
          value -> {
            String stringValue = String.format("%.10f%n", value);
            Utils.packException(
                () -> Files.writeString(history, stringValue, StandardOpenOption.APPEND));
          });
    }
  }

  @Test
  void generateOne() {
    int simulationTime = 100;
    try (Admin admin = Admin.of(bootstrapServers())) {
      var topics = fuzzyCluster(admin, simulationTime);
      var storage = IntStream.range(0, 10).boxed().collect(Collectors.toMap(x -> x, x -> 0L));
      var ingress = false;
      System.out.println("Generate cluster");
      Supplier<Map<Integer, BrokerNetworkLoad>> nextSimulatedLoading =
          () -> simulateLoading2(admin, topics);
      var counter = new AtomicInteger();
      System.out.println("Start");
      Stream.generate(nextSimulatedLoading)
          .map(
              map -> {
                if (ingress) {
                  var maxIngress =
                      map.values().stream()
                          .mapToDouble(
                              x ->
                                  x.ingress
                                      .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS)
                                      .doubleValue())
                          .max()
                          .orElseThrow();
                  var minIngress =
                      map.values().stream()
                          .mapToDouble(
                              x ->
                                  x.ingress
                                      .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS)
                                      .doubleValue())
                          .min()
                          .orElseThrow();
                  return (double) ((maxIngress - minIngress) / minIngress);
                } else {
                  var maxEgress =
                      map.values().stream()
                          .mapToDouble(
                              x ->
                                  x.egress
                                      .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS)
                                      .doubleValue())
                          .max()
                          .orElseThrow();
                  var avgEgress =
                      map.values().stream()
                          .mapToDouble(
                              x ->
                                  x.egress
                                      .toBigDecimal(DataUnit.Byte, ChronoUnit.SECONDS)
                                      .doubleValue())
                          .average()
                          .orElseThrow();
                  return (double) ((maxEgress - avgEgress) / avgEgress);
                }
              })
          .limit(300)
          .forEach(
              x -> {
                int normalized = (int) (x * 10);
                storage.putIfAbsent(normalized, 0L);
                storage.put(normalized, storage.get(normalized) + 1);
                if (counter.incrementAndGet() % 100 == 0) {
                  virtualize(storage);
                  System.out.println();
                  System.out.flush();
                }
              });

      ClusterInfo clusterInfo = admin.clusterInfo(topics);
      var allocation = LayeredClusterLogAllocation.of(clusterInfo);
      var collect = allocation.topicPartitionStream()
          .collect(Collectors.toUnmodifiableMap(
              x -> x,
              allocation::logPlacements));

      Type type = new TypeToken<Map<TopicPartition, List<LogPlacement>>>() {}.getType();
      JsonElement jsonAllocation = gson.toJsonTree(collect, type);
      JsonElement jsonProduce = gson.toJsonTree(theSimulation.produceLoading);
      JsonElement jsonConsume = gson.toJsonTree(theSimulation.consumeLoading);

      JsonObject jsonObject = new JsonObject();
      jsonObject.add("allocation", jsonAllocation);
      jsonObject.add("produce", jsonProduce);
      jsonObject.add("consume", jsonConsume);

      Path path = Path.of("/home/garyparrot/cluster-allocation.json");
      BufferedWriter bufferedWriter = Files.newBufferedWriter(path, StandardOpenOption.CREATE);
      bufferedWriter.write(jsonObject.toString());
      bufferedWriter.close();

      Map<Integer, BrokerNetworkLoad> loadingMap = nextSimulatedLoading.get();
      loadingMap.forEach((broker, load) -> {
        System.out.printf("Broker #%d, [ingress %s] [egress %s]%n", broker,
            load.ingress, load.egress);
      });

      Map<String, DataSize> produceLoad = theSimulation.produceLoading.entrySet().stream()
          .collect(Collectors.groupingBy(x -> x.getKey().topic(), Collectors.mapping(
              Map.Entry::getValue,
              Collectors.reducing(DataUnit.Byte.of(0), DataSize::add))));
      Map<String, DataSize> consumeLoad = theSimulation.consumeLoading.entrySet().stream()
          .collect(Collectors.groupingBy(x -> x.getKey().topic(), Collectors.mapping(
              Map.Entry::getValue,
              Collectors.reducing(DataUnit.Byte.of(0), DataSize::add))));
      Map<String, DataSize> hostResource = Map.of(
          "192.168.103.181", DataUnit.Gb.of(10),
          "192.168.103.182", DataUnit.Gb.of(10));

      Path produceFile = Path.of("/home/garyparrot/produce-inventory.json");
      Path consumeFile = Path.of("/home/garyparrot/consume-inventory.json");
      writeAnsibleLoading(produceFile, "Producer", produceLoad, hostResource);
      writeAnsibleLoading(consumeFile, "Consumer", consumeLoad, hostResource);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
    }
  }

  static void writeAnsibleLoading(Path path, String type, Map<String, DataSize> loading, Map<String, DataSize> hostAndResourceLimit) {
    AtomicInteger loadingNumber = new AtomicInteger();
    List<Map.Entry<String, DataSize>> listOfLoading = loading.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .collect(Collectors.toUnmodifiableList());
    Map<String, Long> remainResources = hostAndResourceLimit.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            x -> x.getValue().measurement(DataUnit.Byte).longValue()));

    JsonObject allLoadingHost = new JsonObject();
    allLoadingHost.add("hosts", new JsonObject());
    AtomicInteger a = new AtomicInteger();
    hostAndResourceLimit.forEach((host, dataLoad) -> {
      int index = a.getAndIncrement();
      JsonObject theHost = new JsonObject();
      theHost.addProperty("ansible_user", "kafka");
      theHost.addProperty("ansible_host", host);
      allLoadingHost.getAsJsonObject("hosts").add("host" + index , theHost);
    });
    JsonObject jsonLoading = new JsonObject();
    jsonLoading.add("hosts", new JsonObject());

    // start allocation algorithm
    listOfLoading.forEach((entry) -> {
      String topic = entry.getKey();
      long bytes = entry.getValue().measurement(DataUnit.Byte).longValue();

      var targetHost = remainResources.entrySet().stream()
          .filter(x -> x.getValue() >= bytes)
          .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
          .findAny()
          .map(Map.Entry::getKey)
          .orElse(null);
      if(targetHost == null) {
        System.out.println("Current Allocation");
        System.out.println(remainResources);
        System.out.println(listOfLoading);
        throw new IllegalStateException("No suitable host can meet resource constraint");
      }

      long now = remainResources.get(targetHost);
      long next = now - bytes;
      remainResources.put(targetHost, next);

      JsonObject theLoad = new JsonObject();
      theLoad.addProperty("ansible_user", "kafka");
      theLoad.addProperty("ansible_host", targetHost);
      theLoad.addProperty("topic_name", topic);
      theLoad.addProperty("throttle", bytes + "Byte");
      int num = loadingNumber.getAndIncrement();
      jsonLoading.getAsJsonObject("hosts").add("loading" + num, theLoad);
    });

    System.out.println("Remaining resource at each worker");
    System.out.println(remainResources);
    System.out.println("Used resource at each worker");
    System.out.println(hostAndResourceLimit.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            x -> x.getValue().subtract(DataUnit.Byte.of(remainResources.get(x.getKey()))))));
    System.out.println();

    JsonObject all = new JsonObject();
    all.add("loading-" + type.toLowerCase() + "-hosts", allLoadingHost);
    all.add("loading-" + type.toLowerCase(), jsonLoading);

    try {
      System.out.println("Write file " + path);
      System.out.println(all);
      if(Files.exists(path))
        Files.delete(path);
      try (BufferedWriter bufferedWriter = Files.newBufferedWriter(path, StandardOpenOption.CREATE)) {
        gson.toJson(all, bufferedWriter);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static void writeAnsibleLoadingByTp(Path path, String type, Map<TopicPartition, DataSize> loading, Map<String, DataSize> hostAndResourceLimit) {
    AtomicInteger loadingNumber = new AtomicInteger();
    List<Map.Entry<String, Map<Integer, DataSize>>> listOfLoading = loading.entrySet().stream()
        .collect(Collectors.groupingBy(x -> x.getKey().topic(),
            Collectors.toMap(
                x -> x.getKey().partition(),
                Map.Entry::getValue)))
        .entrySet()
        .stream()
        .sorted(Comparator.comparing((x) -> x.getValue().values().stream().reduce(DataUnit.Byte.of(0), DataSize::add).bits().doubleValue()))
        .collect(Collectors.toUnmodifiableList());
    Map<String, Long> remainResources = hostAndResourceLimit.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            x -> x.getValue().measurement(DataUnit.Byte).longValue()));

    JsonObject allLoadingHost = new JsonObject();
    allLoadingHost.add("hosts", new JsonObject());
    AtomicInteger a = new AtomicInteger();
    hostAndResourceLimit.forEach((host, dataLoad) -> {
      int index = a.getAndIncrement();
      JsonObject theHost = new JsonObject();
      theHost.addProperty("ansible_user", "kafka");
      theHost.addProperty("ansible_host", host);
      allLoadingHost.getAsJsonObject("hosts").add("host" + index , theHost);
    });
    JsonObject jsonLoading = new JsonObject();
    jsonLoading.add("hosts", new JsonObject());

    // start allocation algorithm
    listOfLoading.forEach((entry) -> {
      String topic = entry.getKey();
      var totalLoad = entry.getValue().values().stream().reduce(DataUnit.Byte.of(0), DataSize::add).measurement(DataUnit.Byte).longValueExact();
      var loadMap = entry.getValue().entrySet().stream()
          .map(e -> String.format("%s-%d=%dByte", topic, e.getKey(), e.getValue().measurement(DataUnit.Byte).longValueExact()))
          .collect(Collectors.joining(","));

      var targetHost = remainResources.entrySet().stream()
          .filter(x -> x.getValue() >= totalLoad)
          .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
          .findAny()
          .map(Map.Entry::getKey)
          .orElse(null);
      if(targetHost == null) {
        System.out.println("Current Allocation");
        System.out.println(remainResources);
        System.out.println(listOfLoading);
        throw new IllegalStateException("No suitable host can meet resource constraint");
      }

      long now = remainResources.get(targetHost);
      long next = now - totalLoad;
      remainResources.put(targetHost, next);

      JsonObject theLoad = new JsonObject();
      theLoad.addProperty("ansible_user", "kafka");
      theLoad.addProperty("ansible_host", targetHost);
      theLoad.addProperty("topic_name", topic);
      theLoad.addProperty("throttle", totalLoad + "Byte");
      theLoad.addProperty("load_map", loadMap);
      int num = loadingNumber.getAndIncrement();
      jsonLoading.getAsJsonObject("hosts").add("loading" + num, theLoad);
    });

    System.out.println("Remaining resource at each worker");
    System.out.println(remainResources);
    System.out.println("Used resource at each worker");
    System.out.println(hostAndResourceLimit.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            x -> x.getValue().subtract(DataUnit.Byte.of(remainResources.get(x.getKey()))))));
    System.out.println();

    JsonObject all = new JsonObject();
    all.add("loading-" + type.toLowerCase() + "-hosts", allLoadingHost);
    all.add("loading-" + type.toLowerCase(), jsonLoading);

    try {
      System.out.println("Write file " + path);
      System.out.println(all);
      if(Files.exists(path))
        Files.delete(path);
      try (BufferedWriter bufferedWriter = Files.newBufferedWriter(path, StandardOpenOption.CREATE)) {
        gson.toJson(all, bufferedWriter);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = "/home/garyparrot/clusters/cluster_15%")
  void applyCluster(String thePlan) {
    // var bootstrap = bootstrapServers();
    var bootstrap = "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655";
    try (Admin admin = Admin.of(bootstrap)) {

      if(!admin.topicNames().isEmpty()) {
        System.out.println("This cluster is not clean: " + admin.topicNames());
      }

      Type type = new TypeToken<Map<String, List<LogPlacement>>>() {}.getType();
      Type type1 = new TypeToken<Map<String, DataSize>>() {}.getType();

      Path path = Path.of(thePlan);
      JsonObject object = gson.fromJson(Files.newBufferedReader(path), JsonObject.class);
      Map<TopicPartition, List<LogPlacement>> allocation = ((Map<String, List<LogPlacement>>)gson.fromJson(object.get("allocation"), type))
          .entrySet().stream()
              .collect(Collectors.toUnmodifiableMap(
                  x -> TopicPartition.of(x.getKey()),
                  Map.Entry::getValue));
      Map<TopicPartition, DataSize> produce = ((Map<String, DataSize>)gson.fromJson(object.get("produce"), type1))
          .entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(
              x -> TopicPartition.of(x.getKey()),
              Map.Entry::getValue));
      Map<TopicPartition, DataSize> consume = ((Map<String, DataSize>)gson.fromJson(object.get("consume"), type1))
          .entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(
              x -> TopicPartition.of(x.getKey()),
              Map.Entry::getValue));

      // reconstruct the allocation
      allocation.entrySet().stream()
          .collect(Collectors.groupingBy(x -> x.getKey().topic(), Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)))
          .forEach((topic, map) -> {
            System.out.printf("Create topic '%s' with %d partitions and %d replicas.%n",
                topic,
                map.size(),
                (short) map.entrySet().iterator().next().getValue().size());
            admin.creator()
                .topic(topic)
                .numberOfPartitions(map.size())
                .numberOfReplicas((short) map.entrySet().iterator().next().getValue().size())
                .create();
          });
      allocation.entrySet().stream()
          .collect(Collectors.groupingBy(x -> x.getKey().topic(), Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)))
          .forEach((topic, map) -> {
            map.forEach((tp, placements) -> {
              admin.migrator()
                  .partition(tp.topic(), tp.partition())
                  .moveTo(placements.stream()
                      .map(LogPlacement::broker)
                      .collect(Collectors.toUnmodifiableList()));
            });
          });
      Utils.sleep(Duration.ofSeconds(5));
      allocation.keySet().forEach(admin::preferredLeaderElection);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class LogPlacementSerializer extends TypeAdapter<LogPlacement> {

    @Override
    public void write(JsonWriter out, LogPlacement value) throws IOException {
      out.beginObject();
      out.name("broker").value(value.broker());
      out.name("logDirectory").value(value.logDirectory().orElse(null));
      out.endObject();
    }

    @Override
    public LogPlacement read(JsonReader in) throws IOException {
      OptionalInt broker = OptionalInt.empty();
      Optional<String> path = Optional.empty();
      in.beginObject();
      while (in.hasNext()) {
        switch (in.nextName()) {
          case "broker":
            broker = OptionalInt.of(in.nextInt());
            break;
          case "logDirectory":
            path = Optional.of(in.nextString());
            break;
        }
      }
      in.endObject();
      return LogPlacement.of(broker.orElseThrow(), path.orElse(null));
    }
  }

  static class DataSizeTypeAdapter extends TypeAdapter<DataSize> {

    @Override
    public void write(JsonWriter out, DataSize value) throws IOException {
      out.value(value.measurement(DataUnit.Byte).longValue());
    }

    @Override
    public DataSize read(JsonReader in) throws IOException {
      return DataUnit.Byte.of(in.nextLong());
    }
  }

  @Test
  void printSimulation() throws IOException {
    var history = Path.of("/home/garyparrot/numbers");
    var lines = Files.readAllLines(history);
    Map<Integer, Long> collect =
        lines.stream()
            .map(Double::parseDouble)
            .collect(Collectors.groupingBy(x -> (int) (x * 100), Collectors.counting()));
    virtualize(collect);
    System.out.println();
    virtualizeCumulative(collect);
  }

  static double standardDeviation(Collection<Double> values) {
    double average = values.stream().mapToDouble(x -> x).average().orElseThrow();
    double variance =
        values.stream().mapToDouble(x -> (x - average) * (x - average)).sum() / values.size();
    return Math.sqrt(variance);
  }

  static class ClusterSimulation {

    final Map<TopicPartition, List<Replica>> relationship;
    final Map<TopicPartition, DataSize> produceLoading;
    final Map<TopicPartition, DataSize> consumeLoading;

    public ClusterSimulation(Map<TopicPartition, List<Replica>> map) {
      this.relationship = map;
      this.produceLoading =
          map.keySet().stream().collect(Collectors.toMap(tp -> tp, tp -> DataUnit.Byte.of(0)));
      this.consumeLoading =
          map.keySet().stream().collect(Collectors.toMap(tp -> tp, tp -> DataUnit.Byte.of(0)));
    }

    public void applyProducerLoading(TopicPartition topicPartition, DataSize dataSize) {
      produceLoading.put(topicPartition, produceLoading.get(topicPartition).add(dataSize));
    }

    public void applyConsumerLoading(TopicPartition topicPartition, DataSize dataSize, int fanout) {
      consumeLoading.put(
          topicPartition, consumeLoading.get(topicPartition).add(dataSize.multiply(fanout)));
    }

    public DataRate calculateBrokerIngress(int brokerId) {
      DataSize zero = DataUnit.Byte.of(0);

      // the ingress from replication or user
      DataSize produceIngress =
          relationship.entrySet().stream()
              .flatMap(e -> e.getValue().stream().map(z -> Map.entry(e.getKey(), z)))
              .filter(e -> e.getValue().broker() == brokerId)
              .map(e -> produceLoading.get(e.getKey()))
              .reduce(zero, DataSize::add);

      return DataRate.of(produceIngress, Duration.ofSeconds(1));
    }

    public DataRate calculateBrokerEgress(int brokerId) {
      DataSize zero = DataUnit.Byte.of(0);

      // egress from consumer
      DataSize consumerEgress =
          relationship.entrySet().stream()
              .flatMap(e -> e.getValue().stream().map(z -> Map.entry(e.getKey(), z)))
              .filter(e -> e.getValue().broker() == brokerId)
              .filter(e -> e.getValue().leader())
              .map(e -> consumeLoading.get(e.getKey()))
              .reduce(zero, DataSize::add);

      // egress from replication
      DataSize replicationEgress =
          relationship.entrySet().stream()
              .flatMap(e -> e.getValue().stream().map(z -> Map.entry(e.getKey(), z)))
              .filter(e -> e.getValue().broker() == brokerId)
              .filter(e -> e.getValue().leader())
              .map(
                  e ->
                      produceLoading
                          .get(e.getKey())
                          .multiply(relationship.get(e.getKey()).size() - 1))
              .reduce(zero, DataSize::add);

      DataSize aggregate = consumerEgress.add(replicationEgress);

      return DataRate.of(aggregate, Duration.ofSeconds(1));
    }
  }

  static class BrokerNetworkLoad {
    public final DataRate ingress;
    public final DataRate egress;

    BrokerNetworkLoad(DataRate ingress, DataRate egress) {
      this.ingress = ingress;
      this.egress = egress;
    }
  }

  static class ClusterFormat {

    private final Map<TopicPartition, List<LogPlacement>> logAllocation;

    ClusterFormat(Map<TopicPartition, List<LogPlacement>> logAllocation) {
      this.logAllocation = logAllocation;
    }

    @Override
    public String toString() {
      Map<String, Map<TopicPartition, List<LogPlacement>>> allocation =
          logAllocation.entrySet().stream()
          .collect(Collectors.groupingBy(x -> x.getKey().topic(), Collectors.toUnmodifiableMap(
              Map.Entry::getKey,
              Map.Entry::getValue)));
      StringBuilder sb = new StringBuilder();

      // format
      // topic,partitionSize,replicaSize,
      for (var entry0: allocation.entrySet()) {
        var topic = entry0.getKey();
        var partitionSize = entry0.getValue().size();
        sb.append(topic).append(",");
        sb.append(partitionSize).append(",");
        for(var entry1: entry0.getValue().entrySet()) {
          var theReplicaSize = entry1.getValue().size();
          var placements = entry1.getValue();
          sb.append(theReplicaSize).append(",");
          for(var placement: placements) {
            String format = String.format("[%d:%s]", placement.broker(), placement.logDirectory());
            sb.append(format).append(",");
          }
        }
        sb.append(System.lineSeparator());
      }

      return sb.toString();
    }
  }

  static class TopicOperation {

    public final Operation operation;
    public final String topicName;
    public final int partitionSize;
    public final int replicaSize;

    TopicOperation(Operation operation, String topicName, int partitionSize, int replicaSize) {
      this.operation = operation;
      this.topicName = topicName;
      this.partitionSize = partitionSize;
      this.replicaSize = replicaSize;
    }

    enum Operation {
      CREATE, DELETE;
    }

  }

  @Test
  void doIt() {
    distributionVisualizer(30000, () -> Math.abs(ThreadLocalRandom.current().nextGaussian()));
  }

}
