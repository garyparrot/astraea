package org.astraea.balancer.alpha.cost;

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
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.argument.Field;
import org.astraea.balancer.alpha.BalancerUtils;
import org.astraea.cost.ClusterInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.topic.TopicAdmin;

public class FolderSizeCost {
  static TopicAdmin admin;
  Map<String, Integer> totalFolderCapacity;

  public FolderSizeCost(Map<String, Integer> totalBrokerCapacity) {
    this.totalFolderCapacity = totalBrokerCapacity;
  }

  public Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo) {

    var usedDiskSpace =
        new TreeMap<TopicPartitionReplica, Double>(
            Comparator.comparing(TopicPartitionReplica::brokerId)
                .thenComparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));

    totalFolderCapacity.forEach(
        (path, totalSize) ->
            admin
                .replicas(admin.topicNames())
                .forEach(
                    (tp, replicas) ->
                        replicas.forEach(
                            r -> {
                              if (r.path().equals(path))
                                usedDiskSpace.put(
                                    new TopicPartitionReplica(
                                        tp.topic(), tp.partition(), r.broker()),
                                    Math.round(
                                            ((double) r.size()
                                                    / totalFolderCapacity.get(path)
                                                    / 1048576)
                                                * 1000.0)
                                        / 1000.0);
                            })));
    return usedDiskSpace;
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  public Fetcher fetcher() {
    return client -> new java.util.ArrayList<>(KafkaMetrics.TopicPartition.Size.fetch(client));
  }

  public static void main(String[] args) throws InterruptedException, MalformedURLException {
    final var argument = org.astraea.argument.Argument.parse(new FolderSizeCost.Argument(), args);
    admin = TopicAdmin.of(argument.brokers);
    var allBeans = new HashMap<Integer, Collection<HasBeanObject>>();
    var jmxAddress = Map.of(1001, 18661, 1002, 18732, 1003, 10474);
    FolderSizeCost costFunction = new FolderSizeCost(argument.totalFolderCapacity);
    jmxAddress.forEach(
        (b, port) ->
            allBeans.put(
                b,
                BeanCollector.builder()
                    .interval(Duration.ofSeconds(4))
                    .build()
                    .register()
                    .host(argument.brokers.split(":")[0])
                    .port(port)
                    .fetcher(Fetcher.of(Set.of(costFunction.fetcher())))
                    .build()
                    .current()));
    ClusterInfo clusterInfo = ClusterInfo.of(BalancerUtils.clusterSnapShot(admin), allBeans);
    costFunction.cost(clusterInfo).forEach((tp, score) -> System.out.println(tp + ":" + score));
  }

  static class Argument extends org.astraea.argument.Argument {
    @Parameter(
        names = {"--folder.capacity.file"},
        description =
            "Path to a java properties file that contains all the total hard disk space(MB) and their corresponding log path",
        converter = FolderCapacityMapField.class,
        required = true)
    Map<String, Integer> totalFolderCapacity;
  }

  static class FolderCapacityMapField extends Field<Map<String, Integer>> {
    static Map.Entry<String, Integer> transformEntry(Map.Entry<String, String> entry) {
      try {
        return Map.entry(entry.getKey(), Integer.parseInt(entry.getValue()));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bad integer format for " + entry.getKey(), e);
      }
    }

    @Override
    public Map<String, Integer> convert(String value) {
      final Properties properties = new Properties();

      try (var reader = Files.newBufferedReader(Path.of(value))) {
        properties.load(reader);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return properties.entrySet().stream()
          .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
          .map(FolderCapacityMapField::transformEntry)
          .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }
}
