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
package org.astraea.common.admin;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.broker.HasGauge;
import org.astraea.common.metrics.broker.LogMetrics;
import org.astraea.common.metrics.broker.ServerMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusterBeanTest {

  @Test
  void testBeans() {
    // BeanObject1 and BeanObject2 is same partition in different broker
    BeanObject testBeanObjectWithPartition1 =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                LogMetrics.Log.SIZE.metricName(),
                "type",
                "Log",
                "topic",
                "testBeans",
                "partition",
                "0"),
            Map.of("Value", 100));
    BeanObject testBeanObjectWithPartition2 =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                LogMetrics.Log.SIZE.metricName(),
                "type",
                "Log",
                "topic",
                "testBeans",
                "partition",
                "0"),
            Map.of("Value", 100));
    BeanObject testBeanObjectWithPartition3 =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                LogMetrics.Log.LOG_END_OFFSET.name(),
                "type",
                "Log",
                "topic",
                "testBeans",
                "partition",
                "0"),
            Map.of("Value", 100));
    BeanObject testBeanObjectWithoutPartition =
        new BeanObject(
            "kafka.log",
            Map.of(
                "name",
                ServerMetrics.ReplicaManager.LEADER_COUNT.metricName(),
                "type",
                "ReplicaManager"),
            Map.of("Value", 300));
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1,
                List.of(HasGauge.ofLong(testBeanObjectWithPartition1)),
                2,
                List.of(
                    HasGauge.ofLong(testBeanObjectWithoutPartition),
                    HasGauge.ofLong(testBeanObjectWithPartition2),
                    HasGauge.ofLong(testBeanObjectWithPartition3))));
    // test all
    Assertions.assertEquals(2, clusterBean.all().size());
    Assertions.assertEquals(1, clusterBean.all().get(1).size());
    Assertions.assertEquals(3, clusterBean.all().get(2).size());

    // test get beanObject by replica
    Assertions.assertEquals(2, clusterBean.mapByReplica().size());
    Assertions.assertEquals(
        2, clusterBean.mapByReplica().get(TopicPartitionReplica.of("testBeans", 0, 2)).size());
  }

  final List<String> topicNamePool =
      IntStream.range(0, 100)
          .mapToObj(i -> "topic-" + Utils.randomString())
          .collect(Collectors.toUnmodifiableList());

  Stream<HasBeanObject> random(int broker) {
    final var time = new AtomicLong();
    return Stream.generate(
            () -> {
              final var t = time.incrementAndGet();
              final var domainName = "kafka.server";
              final var properties =
                  Map.of(
                      "type",
                      "BrokerTopicMetrics",
                      "broker",
                      String.valueOf(broker),
                      "topic",
                      topicNamePool.get((int) (t % 100)),
                      "name",
                      "BytesInPerSec");
              final var attributes =
                  Map.<String, Object>of("count", ThreadLocalRandom.current().nextInt());
              final var bean = new BeanObject(domainName, properties, attributes);
              return Map.entry(t, bean);
            })
        .map(
            e -> {
              final var fakeTime = (long) e.getKey();
              switch (((int) fakeTime) % 3) {
                case 0:
                  return new Metric1(fakeTime, e.getValue());
                case 1:
                  return new Metric2(fakeTime, e.getValue());
                case 2:
                  return new Metric3(fakeTime, e.getValue());
                default:
                  throw new RuntimeException();
              }
            });
  }

  @Test
  void testLatestQuery() {
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1, random(1).limit(1000).collect(Collectors.toUnmodifiableList()),
                2, random(2).limit(1000).collect(Collectors.toUnmodifiableList()),
                3, random(3).limit(1000).collect(Collectors.toUnmodifiableList())));

    // fetch the latest metric from one broker
    Metric2 firstMetric =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric2.class, Set.of(1))
                .useLatestQuery()
                .descendingOrder()
                .build());
    Assertions.assertInstanceOf(Metric2.class, firstMetric);
    Assertions.assertEquals(1000, firstMetric.createdTimestamp());

    // fetch the latest metric from any of the brokers
    Metric1 firstAnyMetric =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric1.class, Set.of(1, 2))
                .useLatestQuery()
                .descendingOrder()
                .build());
    Assertions.assertInstanceOf(Metric1.class, firstAnyMetric);
    Assertions.assertNotEquals(3, firstMetric.broker());

    // fetch the oldest metric of all the brokers
    Map<Integer, Metric3> firstBrokerMetric =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric3.class, Set.of(1, 2, 3))
                .useLatestQuery()
                .groupingBy((id, bean) -> id)
                .ascendingOrder()
                .build());
    firstBrokerMetric.forEach(
        (id, metric) -> {
          Assertions.assertInstanceOf(Metric3.class, metric);
          Assertions.assertEquals(id, metric.broker());
          Assertions.assertEquals(2, metric.createdTimestamp());
        });

    // grouping demo
    Map<String, Metric1> grouping =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric1.class, Set.of(1, 2, 3))
                .useLatestQuery()
                .groupingBy((id, bean) -> "Broker" + id + "_" + bean.topic())
                .descendingOrder()
                .build());
    grouping.forEach(
        (brokerTopic, metric) -> {
          Assertions.assertEquals(300, grouping.size());
          Assertions.assertTrue(brokerTopic.matches("Broker[1-3]_topic-.+"));
        });

    // grouping demo 2
    Map<String, Metric1> grouping2 =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric1.class, Set.of(1, 2, 3))
                .useLatestQuery()
                .groupingBy((id, bean) -> bean.topic())
                .descendingOrder()
                .build());
    grouping2.forEach(
        (brokerTopic, metric) -> {
          Assertions.assertEquals(100, grouping2.size());
          Assertions.assertTrue(brokerTopic.matches("topic-.+"));
        });
  }

  @Test
  void testWindowQuery() {
    var clusterBean =
        ClusterBean.of(
            Map.of(
                1, random(1).limit(1000).collect(Collectors.toUnmodifiableList()),
                2, random(2).limit(1000).collect(Collectors.toUnmodifiableList()),
                3, random(3).limit(1000).collect(Collectors.toUnmodifiableList())));

    List<Metric1> all =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric1.class, Set.of(1, 2, 3))
                .useWindowQuery()
                .descendingOrder()
                .build());
    Assertions.assertEquals(999, all.size());
    Assertions.assertEquals(
        all.stream()
            .sorted(Comparator.comparingLong(Metric1::createdTimestamp).reversed())
            .collect(Collectors.toUnmodifiableList()),
        all);

    List<Metric2> filtered =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric2.class, Set.of(1, 2))
                .useWindowQuery()
                .filterByTime(500)
                .filterBy(bean -> bean.count() > 0)
                .build());
    Assertions.assertTrue(filtered.stream().allMatch(bean -> bean.createdTimestamp() >= 500));
    Assertions.assertTrue(filtered.stream().allMatch(bean -> bean.count() > 0));
    Assertions.assertTrue(filtered.stream().allMatch(bean -> Set.of(1, 2).contains(bean.broker())));

    Map<Integer, List<Metric3>> allMetric3 =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric3.class, Set.of(1, 2, 3))
                .useWindowQuery()
                .groupingBy((id, metric) -> id)
                .filterByTime(500)
                .descendingOrder()
                .build());
    Assertions.assertEquals(Set.of(1, 2, 3), allMetric3.keySet());
    allMetric3.forEach(
        (id, metrics) -> {
          Assertions.assertTrue(metrics.stream().allMatch(bean -> bean.createdTimestamp() >= 500));
          Assertions.assertEquals(
              metrics.stream()
                  .sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp).reversed())
                  .collect(Collectors.toUnmodifiableList()),
              metrics);
          metrics.forEach(m -> Assertions.assertInstanceOf(Metric3.class, m));
        });

    Map<String, List<Metric3>> grouping =
        clusterBean.run(
            ClusterBeanQuery.builder(Metric3.class, Set.of(1, 2, 3))
                .useWindowQuery()
                .groupingBy((id, bean) -> "Broker" + id + "_" + bean.topic())
                .ascendingOrder()
                .build());
    Assertions.assertEquals(300, grouping.size());
    Assertions.assertTrue(
        grouping.keySet().stream().allMatch(key -> key.matches("Broker[1-3]_topic-.+")));
    grouping
        .values()
        .forEach(
            metrics ->
                Assertions.assertEquals(
                    metrics.stream()
                        .sorted(Comparator.comparingLong(HasBeanObject::createdTimestamp))
                        .collect(Collectors.toUnmodifiableList()),
                    metrics));
  }

  private static class Metric1 implements HasBeanObject {
    private final long createTime;
    private final BeanObject beanObject;

    private Metric1(long createTime, BeanObject beanObject) {
      this.createTime = createTime;
      this.beanObject = beanObject;
    }

    public int broker() {
      return Integer.parseInt(beanObject.properties().get("broker"));
    }

    public int count() {
      return (int) beanObject.attributes().get("count");
    }

    public String topic() {
      return beanObject.properties().get("topic");
    }

    @Override
    public BeanObject beanObject() {
      return beanObject;
    }

    @Override
    public long createdTimestamp() {
      return createTime;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName()
          + "{"
          + "createTime="
          + createTime
          + ", beanObject="
          + beanObject
          + '}';
    }
  }

  private static class Metric2 extends Metric1 {
    private Metric2(long createTime, BeanObject beanObject) {
      super(createTime, beanObject);
    }
  }

  private static class Metric3 extends Metric2 {
    private Metric3(long createTime, BeanObject beanObject) {
      super(createTime, beanObject);
    }
  }
}
