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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.argument.Field;
import org.astraea.balancer.alpha.BalancerUtils;
import org.astraea.cost.ClusterInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.topic.TopicAdmin;

public class ReplicaDiskInCost implements CostFunction {
  Map<Integer, Integer> brokerBandwidthCap;

  public ReplicaDiskInCost(Map<Integer, Integer> brokerBandwidthCap) {
    this.brokerBandwidthCap = brokerBandwidthCap;
  }

  @Override
  public Map<TopicPartitionReplica, Double> cost(ClusterInfo clusterInfo)
      throws InterruptedException {
    // a replica average data in rate (MB/s).
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
    return dataInRate;
  }

  /** @return the metrics getters. Those getters are used to fetch mbeans. */
  @Override
  public Fetcher fetcher() {
    return client -> new java.util.ArrayList<>(KafkaMetrics.TopicPartition.Size.fetch(client));
  }

  public Map<TopicPartitionReplica, Double> replicaInCount(ClusterInfo clusterInfo) {
    Map<TopicPartitionReplica, List<HasBeanObject>> tpBeanObjects = new HashMap();
    clusterInfo
        .allBeans()
        .forEach(
            ((broker, beanObjects) -> {
              clusterInfo
                  .topics()
                  .forEach(
                      topic -> {
                        clusterInfo
                            .partitions(topic)
                            .forEach(
                                partitionInfo -> {
                                  tpBeanObjects.put(
                                      new TopicPartitionReplica(
                                          partitionInfo.topic(), partitionInfo.partition(), broker),
                                      beanObjects.stream()
                                          .filter(
                                              beanObject ->
                                                  beanObject
                                                          .beanObject()
                                                          .getProperties()
                                                          .get("topic")
                                                          .equals(partitionInfo.topic())
                                                      && Integer.parseInt(
                                                              beanObject
                                                                  .beanObject()
                                                                  .getProperties()
                                                                  .get("partition"))
                                                          == (partitionInfo.partition()))
                                          .collect(Collectors.toList()));
                                });
                      });
            }));
    Map<TopicPartitionReplica, Double> replicaIn = new HashMap<>();
    tpBeanObjects.forEach(
        (tpr, beanObjects) -> {
            var sortedBeanObjects = beanObjects.stream().sorted(Comparator.comparing(HasBeanObject::createdTimestamp,Comparator.reverseOrder())).collect(Collectors.toList());
            var duration = 2;
            if (sortedBeanObjects.size() <= duration)
            {
               // throw new IllegalStateException("");
            }else {
                var beanObjectNew = sortedBeanObjects.get(0).beanObject();
                var beanObjectOld =  sortedBeanObjects.get(duration-1).beanObject();
                var a=(double)
                        ( (long) beanObjectNew.getAttributes().get("Value") -
                                (long) beanObjectOld.getAttributes().get("Value"))
                        / ((beanObjectNew.createdTimestamp()-beanObjectOld.createdTimestamp())/1000)
                        * clusterInfo.beans(tpr.brokerId()).size()
                        / 1048576.0;
                replicaIn.put(
                        tpr,a);
        }
        });
    return replicaIn;
  }

  public static void main(String[] args) throws InterruptedException, MalformedURLException {
    final var argument =
        org.astraea.argument.Argument.parse(new ReplicaDiskInCost.Argument(), args);
    var admin = TopicAdmin.of(argument.brokers);
    var allBeans = new HashMap<Integer, Collection<HasBeanObject>>();
    var jmxAddress = Map.of(1001, 15629, 1002, 10585, 1003, 12485);
    // set broker bandwidth upper limit to 10 MB/s;
    CostFunction costFunction = new ReplicaDiskInCost(argument.brokerBandwidthCap);

    for (var i = 1; i <= 2; i++) {
      jmxAddress.forEach(
          (b, port) -> {
            var firstBeanObjects =
                BeanCollector.builder()
                    .interval(Duration.ofSeconds(4))
                    .build()
                    .register()
                    .host(argument.brokers.split(":")[0])
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
      if (i == 1)
        try {
          TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
    }

    ClusterInfo clusterInfo = ClusterInfo.of(BalancerUtils.clusterSnapShot(admin), allBeans);
    costFunction.cost(clusterInfo).forEach((tp, score) -> System.out.println(tp + ":" + score));
  }

  static class Argument extends org.astraea.argument.Argument {
    @Parameter(
        names = {"--broker.bandwidthCap.file"},
        description = "",
        converter = brokerBandwidthCapMapField.class,
        required = true)
    Map<Integer, Integer> brokerBandwidthCap;

    static class brokerBandwidthCapMapField extends Field<Map<Integer, Integer>> {
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
            .map(brokerBandwidthCapMapField::transformEntry)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    }
  }
}
