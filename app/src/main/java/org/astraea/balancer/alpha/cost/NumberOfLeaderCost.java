package org.astraea.balancer.alpha.cost;

import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.balancer.alpha.BalancerUtils;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.astraea.topic.TopicAdmin;

public class NumberOfLeaderCost implements HasBrokerCost {
  @Override
  public Fetcher fetcher() {
    return client ->
        new java.util.ArrayList<>(KafkaMetrics.ReplicaManager.LeaderCount.fetch(client));
  }

  /**
   * @param clusterInfo cluster information
   * @return a BrokerCost contain all ratio of leaders that exist on all brokers
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    Map<Integer, Integer> leaderCount = new HashMap<>();
    Map<Integer, Double> leaderCost = new HashMap<>();
    clusterInfo
        .allBeans()
        .forEach(
            (key, value) ->
                value.stream()
                    .filter(x -> x instanceof HasValue)
                    .filter(x -> x.beanObject().getProperties().get("name").equals("LeaderCount"))
                    .filter(
                        x -> x.beanObject().getProperties().get("type").equals("ReplicaManager"))
                    .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
                    .map(x -> (HasValue) x)
                    .limit(1)
                    .forEach(hasValue -> leaderCount.put(key, (int) hasValue.value())));
    var totalLeader = leaderCount.values().stream().mapToInt(Integer::intValue).sum();
    leaderCount.forEach(
        (broker, leaderNum) -> {
          leaderCost.put(broker, (double) leaderNum / totalLeader);
        });
    return () -> leaderCost;
  }

  public static void main(String[] args) throws InterruptedException, MalformedURLException {
    var host = "localhost";
    var brokerPort = 14179;
    var admin = TopicAdmin.of(host + ":" + brokerPort);
    var allBeans = new HashMap<Integer, Collection<HasBeanObject>>();
    var jmxAddress = Map.of(1001, 11040, 1002, 15006, 1003, 10059);

    NumberOfLeaderCost costFunction = new NumberOfLeaderCost();
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
        .forEach((broker, score) -> System.out.println(broker + ":" + score));
  }
}
