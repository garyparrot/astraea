package org.astraea.cost.broker;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;

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
    var leaderCount =
        clusterInfo.allBeans().entrySet().stream()
            .flatMap(
                e ->
                    e.getValue().stream()
                        .filter(x -> x instanceof HasValue)
                        .filter(
                            x -> x.beanObject().getProperties().get("name").equals("LeaderCount"))
                        .filter(
                            x ->
                                x.beanObject().getProperties().get("type").equals("ReplicaManager"))
                        .sorted(Comparator.comparing(HasBeanObject::createdTimestamp).reversed())
                        .map(x -> (HasValue) x)
                        .limit(1)
                        .map(e2 -> Map.entry(e.getKey(), (int) e2.value())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    var totalLeader = leaderCount.values().stream().mapToInt(Integer::intValue).sum();
    var leaderCost =
        leaderCount.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (double) e.getValue() / totalLeader));
    return () -> leaderCost;
  }
}
