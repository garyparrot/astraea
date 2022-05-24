package org.astraea.cost;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.HasValue;
import org.astraea.metrics.kafka.KafkaMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ReplicaDiskInCostTest {

  @Test
  void testPartitionCost() {
    Map<Integer, Integer> brokerBandwidth = new HashMap<>();
    brokerBandwidth.put(1, 50);
    brokerBandwidth.put(2, 50);
    brokerBandwidth.put(3, 50);
    var loadCostFunction = new ReplicaDiskInCost(brokerBandwidth);
    var broker1ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(1);
    var broker2ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(2);
    var broker3ReplicaLoad = loadCostFunction.partitionCost(exampleClusterInfo()).value(3);
    // broker1
    Assertions.assertEquals(
        0.23841381072998047,
        broker1ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-1", 0)));
    Assertions.assertEquals(
        0.1907339096069336,
        broker1ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-2", 0)));
    // broker2
    Assertions.assertEquals(
        0.23841381072998047,
        broker2ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-1", 0)));
    Assertions.assertEquals(
        0.476834774017334, broker2ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-1", 1)));
    // broker3
    Assertions.assertEquals(
        0.476834774017334, broker3ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-1", 1)));
    Assertions.assertEquals(
        0.1907339096069336,
        broker3ReplicaLoad.get(org.astraea.cost.TopicPartition.of("test-2", 0)));
  }

  @Test
  void testBrokerCost() {
    Map<Integer, Integer> properties = new HashMap<>();
    properties.put(1, 50);
    properties.put(2, 50);
    properties.put(3, 50);
    var loadCostFunction = new ReplicaDiskInCost(properties);
    var brokerLoad = loadCostFunction.brokerCost(exampleClusterInfo()).value();
    Assertions.assertEquals(0.23841381072998047 + 0.1907339096069336, brokerLoad.get(1));
    Assertions.assertEquals(0.23841381072998047 + 0.476834774017334, brokerLoad.get(2));
    Assertions.assertEquals(0.476834774017334 + 0.1907339096069336, brokerLoad.get(3));
  }

  private ClusterInfo exampleClusterInfo() {
    var oldTP1_0 =
        mockResult(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "0", 1000, 1000L);
    var newTP1_0 =
        mockResult(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "0", 50000000, 5000L);
    var oldTP1_1 =
        mockResult("Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "1", 500, 1000L);
    var newTP1_1 =
        mockResult(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-1", "1", 100000000, 5000L);
    var oldTP2_0 =
        mockResult("Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "0", 200, 1000L);
    var newTP2_0 =
        mockResult(
            "Log", KafkaMetrics.TopicPartition.Size.metricName(), "test-2", "0", 40000000, 5000L);
    /*
    test replica distribution :
        broker1 : test-1-0,test-2-0
        broker2 : test-1-0,test-1-1
        broker1 : test-1-1,test-2-0
     */
    Collection<HasBeanObject> broker1 = List.of(oldTP1_0, newTP1_0, oldTP2_0, newTP2_0);
    Collection<HasBeanObject> broker2 = List.of(oldTP1_0, newTP1_0, oldTP1_1, newTP1_1);
    Collection<HasBeanObject> broker3 = List.of(oldTP1_1, newTP1_1, oldTP2_0, newTP2_0);
    return new FakeClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1));
      }

      @Override
      public Set<String> topics() {
        return Set.of("test-1", "test-2");
      }

      @Override
      public List<PartitionInfo> partitions(String topic) {
        if (topic.equals("test-1"))
          return List.of(
              PartitionInfo.of(
                  "test-1",
                  0,
                  null,
                  List.of(NodeInfo.of(1, "", -1), NodeInfo.of(2, "", -1)),
                  null,
                  null),
              PartitionInfo.of(
                  "test-1",
                  1,
                  null,
                  List.of(NodeInfo.of(2, "", -1), NodeInfo.of(3, "", -1)),
                  null,
                  null));
        else
          return List.of(
              PartitionInfo.of(
                  "test-2",
                  0,
                  null,
                  List.of(NodeInfo.of(1, "", -1), NodeInfo.of(3, "", -1)),
                  null,
                  null));
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Map.of(1, broker1, 2, broker2, 3, broker3);
      }
    };
  }

  private HasValue mockResult(
      String type, String name, String topic, String partition, long size, long time) {
    var result = Mockito.mock(HasValue.class);
    var bean = Mockito.mock(BeanObject.class);
    Mockito.when(result.beanObject()).thenReturn(bean);
    Mockito.when(result.createdTimestamp()).thenReturn(time);
    Mockito.when(bean.getProperties())
        .thenReturn(Map.of("name", name, "type", type, "topic", topic, "partition", partition));

    Mockito.when(result.value()).thenReturn(size);
    return result;
  }
}
