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
package org.astraea.app.balancer.alpha.cost;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.argument.Field;
import org.astraea.app.balancer.alpha.BalancerUtils;
import org.astraea.app.cost.BrokerCost;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.HasPartitionCost;
import org.astraea.app.cost.PartitionCost;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.BeanCollector;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.kafka.HasValue;
import org.astraea.app.metrics.kafka.KafkaMetrics;

public class ReplicaSizeCost implements HasBrokerCost, HasPartitionCost {
  Map<Integer, Integer> totalBrokerCapacity;

  public ReplicaSizeCost(Map<Integer, Integer> totalBrokerCapacity) {
    this.totalBrokerCapacity = totalBrokerCapacity;
  }

  @Override
  public Fetcher fetcher() {
    return client -> new java.util.ArrayList<>(KafkaMetrics.TopicPartition.Size.fetch(client));
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the ratio of the used space to the free space of each broker
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var sizeOfReplica = getReplicaSize(clusterInfo);
    var totalReplicaSizeInBroker = new HashMap<Integer, Long>();
    var brokerSizeScore = new HashMap<Integer, Double>();
    clusterInfo
        .nodes()
        .forEach(
            nodeInfo ->
                totalReplicaSizeInBroker.put(
                    nodeInfo.id(),
                    sizeOfReplica.entrySet().stream()
                        .filter(tpr -> tpr.getKey().brokerId() == nodeInfo.id())
                        .mapToLong(Map.Entry::getValue)
                        .sum()));
    totalReplicaSizeInBroker.forEach(
        (broker, score) -> {
          brokerSizeScore.put(
              broker, Double.valueOf(score) / totalBrokerCapacity.get(broker) / 1048576);
        });
    return () -> brokerSizeScore;
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a PartitionCost contain ratio of space used by replicas in all brokers
   */
  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo) {
    var sizeOfReplica = getReplicaSize(clusterInfo);
    TreeMap<TopicPartitionReplica, Double> replicaCost =
        new TreeMap<>(
            Comparator.comparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition)
                .thenComparing(TopicPartitionReplica::brokerId));

    sizeOfReplica.forEach(
        (tpr, size) ->
            replicaCost.put(
                tpr, (double) size / totalBrokerCapacity.get(tpr.brokerId()) / 1048576));
    return new PartitionCost() {

      @Override
      public Map<TopicPartition, Double> value(String topic) {
        return clusterInfo.replicas(topic).stream()
            .map(
                partitionInfo ->
                    TopicPartition.of(
                        partitionInfo.topic(), Integer.toString(partitionInfo.partition())))
            .map(
                tp -> {
                  final var score =
                      replicaCost.entrySet().stream()
                          .filter(
                              x ->
                                  x.getKey().topic().equals(tp.topic())
                                      && (x.getKey().partition() == tp.partition()))
                          .mapToDouble(Map.Entry::getValue)
                          .max()
                          .orElseThrow(
                              () ->
                                  new IllegalStateException(
                                      tp + " topic/partition size not found"));
                  return Map.entry(tp, score);
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
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
                    x ->
                        TopicPartition.of(
                            x.getKey().topic(), Integer.toString(x.getKey().partition())),
                    Map.Entry::getValue));
      }
    };
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a map contain the replica log size of each topic/partition
   */
  public Map<TopicPartitionReplica, Long> getReplicaSize(ClusterInfo clusterInfo) {
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
    final var argument =
        org.astraea.app.argument.Argument.parse(new ReplicaSizeCost.Argument(), args);
    var host = "localhost";
    var brokerPort = 19670;
    var admin = Admin.of(host + ":" + brokerPort);
    var allBeans = new HashMap<Integer, Collection<HasBeanObject>>();
    var jmxAddress = Map.of(1001, 11790, 1002, 10818, 1003, 10929);
    ReplicaSizeCost costFunction = new ReplicaSizeCost(argument.totalBrokerCapacity);
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
        .brokerCost(clusterInfo)
        .value()
        .forEach((broker, score) -> System.out.println((broker) + ":" + score));
    System.out.println("");
    costFunction
        .partitionCost(clusterInfo)
        .value(1001)
        .forEach(
            (tp, score) -> System.out.println(tp.topic() + "-" + tp.partition() + ":" + score));
    System.out.println("");
    costFunction
        .partitionCost(clusterInfo)
        .value("test-1")
        .forEach(
            (tp, score) -> {
              System.out.println(tp.topic() + " " + tp.partition() + ": " + score);
            });
  }

  static class Argument extends org.astraea.app.argument.Argument {
    @Parameter(
        names = {"--broker.capacity.file"},
        description =
            "Path to a java properties file that contains all the total hard disk space(MB) and their corresponding log path",
        converter = BrokerBandwidthCapMapField.class,
        required = true)
    Map<Integer, Integer> totalBrokerCapacity;
  }

  static class BrokerBandwidthCapMapField extends Field<Map<Integer, Integer>> {
    static final Pattern serviceUrlKeyPattern =
        Pattern.compile("broker\\.(?<brokerId>[1-9][0-9]{0,9})");

    static Map.Entry<Integer, Integer> transformEntry(Map.Entry<String, String> entry) {
      final Matcher matcher = serviceUrlKeyPattern.matcher(entry.getKey());
      if (matcher.matches()) {
        try {
          int brokerId = Integer.parseInt(matcher.group("brokerId"));
          return Map.entry(brokerId, Integer.parseInt(entry.getValue()));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Bad integer format for " + entry.getKey(), e);
        }
      } else {
        throw new IllegalArgumentException(
            "Bad key format for "
                + entry.getKey()
                + " no match for the following format :"
                + serviceUrlKeyPattern.pattern());
      }
    }

    @Override
    public Map<Integer, Integer> convert(String value) {
      final Properties properties = new Properties();

      try (var reader = Files.newBufferedReader(Path.of(value))) {
        properties.load(reader);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return properties.entrySet().stream()
          .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
          .map(BrokerBandwidthCapMapField::transformEntry)
          .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }
}
