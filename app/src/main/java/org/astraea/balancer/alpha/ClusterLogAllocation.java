package org.astraea.balancer.alpha;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.cost.TopicPartition;

public class ClusterLogAllocation implements Map<TopicPartition, List<LogPlacement>> {

  // TODO: add a method to calculate the difference between two ClusterLogAllocation

  private final Map<TopicPartition, List<LogPlacement>> allocation;

  private ClusterLogAllocation(Map<TopicPartition, List<LogPlacement>> allocation) {
    this.allocation = allocation;
  }

  public static ClusterLogAllocation of(Map<TopicPartition, List<LogPlacement>> allocation) {
    return new ClusterLogAllocation(allocation);
  }

  @Override
  public int size() {
    return allocation.size();
  }

  @Override
  public boolean isEmpty() {
    return allocation.isEmpty();
  }

  @Override
  public boolean containsKey(Object o) {
    return allocation.containsKey(o);
  }

  @Override
  public boolean containsValue(Object o) {
    return allocation.containsValue(o);
  }

  @Override
  public List<LogPlacement> get(Object o) {
    return allocation.get(o);
  }

  @Override
  public List<LogPlacement> put(TopicPartition topicPartition, List<LogPlacement> logPlacements) {
    return allocation.put(topicPartition, logPlacements);
  }

  @Override
  public List<LogPlacement> remove(Object o) {
    return allocation.remove(o);
  }

  @Override
  public void putAll(Map<? extends TopicPartition, ? extends List<LogPlacement>> map) {
    allocation.putAll(map);
  }

  @Override
  public void clear() {
    allocation.clear();
  }

  @Override
  public Set<TopicPartition> keySet() {
    return allocation.keySet();
  }

  @Override
  public Collection<List<LogPlacement>> values() {
    return allocation.values();
  }

  @Override
  public Set<Entry<TopicPartition, List<LogPlacement>>> entrySet() {
    return allocation.entrySet();
  }
}
