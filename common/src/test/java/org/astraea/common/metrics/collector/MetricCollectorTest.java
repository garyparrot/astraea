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
package org.astraea.common.metrics.collector;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.platform.HostMetrics;
import org.astraea.common.metrics.platform.JvmMemory;
import org.astraea.common.metrics.platform.OperatingSystemInfo;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

class MetricCollectorTest extends RequireBrokerCluster {

  private static final Fetcher memoryFetcher = (client) -> List.of(HostMetrics.jvmMemory(client));
  private static final Fetcher osFetcher = (client) -> List.of(HostMetrics.operatingSystem(client));

  @Test
  void addFetcher() {
    try (var collector = MetricCollector.builder().build()) {
      collector.addFetcher(memoryFetcher);

      Assertions.assertEquals(1, collector.listFetchers().size());
      Assertions.assertTrue(collector.listFetchers().contains(memoryFetcher));
    }
  }

  @Test
  void registerJmx() {
    try (var collector = MetricCollector.builder().build()) {
      var socket =
          InetSocketAddress.createUnresolved(jmxServiceURL().getHost(), jmxServiceURL().getPort());
      collector.registerJmx(1, socket);
      collector.registerLocalJmx(-1);

      Assertions.assertEquals(2, collector.listIdentities().size());
      Assertions.assertTrue(collector.listIdentities().contains(1));
      Assertions.assertTrue(collector.listIdentities().contains(-1));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> collector.registerJmx(-1, socket),
          "The id -1 is already registered");
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> collector.registerLocalJmx(-1),
          "The id -1 is already registered");
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  void allMetrics() {
    var sample = Duration.ofMillis(200);
    try (var collector = MetricCollector.builder().interval(sample).build()) {
      collector.registerLocalJmx(0);
      collector.registerLocalJmx(1);
      collector.registerLocalJmx(2);
      collector.addFetcher(memoryFetcher);
      collector.addFetcher(osFetcher);

      Utils.sleep(sample);
      Utils.sleep(sample);

      var memory = collector.allMetrics(JvmMemory.class);
      var os = collector.allMetrics(OperatingSystemInfo.class);

      Assertions.assertEquals(3, memory.keySet().size());
      Assertions.assertTrue(memory.get(0).size() > 0, "has progress");
      Assertions.assertTrue(memory.get(1).size() > 0, "has progress");
      Assertions.assertTrue(memory.get(2).size() > 0, "has progress");
      Assertions.assertTrue(memory.get(0).stream().allMatch(x -> x instanceof JvmMemory));
      Assertions.assertTrue(memory.get(1).stream().allMatch(x -> x instanceof JvmMemory));
      Assertions.assertTrue(memory.get(2).stream().allMatch(x -> x instanceof JvmMemory));

      Assertions.assertEquals(3, os.keySet().size());
      Assertions.assertTrue(os.get(0).size() > 0, "has progress");
      Assertions.assertTrue(os.get(1).size() > 0, "has progress");
      Assertions.assertTrue(os.get(2).size() > 0, "has progress");
      Assertions.assertTrue(os.get(0).stream().allMatch(x -> x instanceof OperatingSystemInfo));
      Assertions.assertTrue(os.get(1).stream().allMatch(x -> x instanceof OperatingSystemInfo));
      Assertions.assertTrue(os.get(2).stream().allMatch(x -> x instanceof OperatingSystemInfo));
    }
  }

  @Test
  void clusterBean() {
    var sample = Duration.ofMillis(200);
    try (var collector = MetricCollector.builder().interval(sample).build()) {
      collector.registerLocalJmx(0);
      collector.registerLocalJmx(1);
      collector.registerLocalJmx(2);
      collector.addFetcher(memoryFetcher);
      collector.addFetcher(osFetcher);

      Utils.sleep(sample);
      Utils.sleep(sample);

      ClusterBean clusterBean = collector.clusterBean();

      Assertions.assertEquals(3, clusterBean.all().keySet().size());
      Assertions.assertTrue(
          clusterBean.all().get(0).stream().anyMatch(x -> x instanceof JvmMemory));
      Assertions.assertTrue(
          clusterBean.all().get(1).stream().anyMatch(x -> x instanceof JvmMemory));
      Assertions.assertTrue(
          clusterBean.all().get(2).stream().anyMatch(x -> x instanceof JvmMemory));
      Assertions.assertTrue(
          clusterBean.all().get(0).stream().anyMatch(x -> x instanceof OperatingSystemInfo));
      Assertions.assertTrue(
          clusterBean.all().get(1).stream().anyMatch(x -> x instanceof OperatingSystemInfo));
      Assertions.assertTrue(
          clusterBean.all().get(2).stream().anyMatch(x -> x instanceof OperatingSystemInfo));
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  void metrics() {
    var sample = Duration.ofMillis(300);
    try (var collector = MetricCollector.builder().interval(sample).build()) {
      collector.addFetcher(memoryFetcher);
      collector.addFetcher(osFetcher);
      collector.registerLocalJmx(0);

      var start = System.currentTimeMillis();
      Utils.sleep(sample.dividedBy(2));
      var untilNow = System.currentTimeMillis();

      Supplier<Stream<JvmMemory>> memory = () -> collector.metrics(JvmMemory.class, 0, start);
      Supplier<Stream<OperatingSystemInfo>> os =
          () -> collector.metrics(OperatingSystemInfo.class, 0, start);

      Assertions.assertEquals(1, memory.get().count());
      Assertions.assertTrue(memory.get().allMatch(x -> x instanceof JvmMemory));
      Assertions.assertTrue(
          memory.get().findFirst().orElseThrow().createdTimestamp() < untilNow,
          "Sampled before the next interval");

      Assertions.assertEquals(1, os.get().count());
      Assertions.assertTrue(os.get().allMatch(x -> x instanceof OperatingSystemInfo));
      Assertions.assertTrue(
          os.get().findFirst().orElseThrow().createdTimestamp() < untilNow,
          "Sampled before the next interval");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {100, 200, 500, 1000})
  void storageSize(int sampleInterval) {
    try (var collector =
        MetricCollector.builder().interval(Duration.ofMillis(sampleInterval)).build()) {
      collector.addFetcher(memoryFetcher);
      collector.registerLocalJmx(-1);

      Utils.sleep(Duration.ofMillis(sampleInterval));
      Utils.sleep(Duration.ofMillis(sampleInterval));

      Assertions.assertTrue(collector.storageSize(-1) > 0, "There are some metrics");
    }
  }

  @Test
  void close() {
    List<MBeanClient> clients = new ArrayList<>();
    List<ScheduledExecutorService> services = new ArrayList<>();
    try (var ignore0 = Mockito.mockStatic(MBeanClient.class, sniff("jndi", clients))) {
      try (var ignore1 =
          Mockito.mockStatic(Executors.class, sniff("newScheduledThreadPool", services))) {
        var socket =
            InetSocketAddress.createUnresolved(
                jmxServiceURL().getHost(), jmxServiceURL().getPort());
        var collector = MetricCollector.builder().build();
        collector.addFetcher(memoryFetcher);
        collector.addFetcher(memoryFetcher);
        collector.addFetcher(memoryFetcher);
        collector.registerJmx(0, socket);
        collector.registerJmx(1, socket);
        collector.registerJmx(2, socket);

        // client & service are created
        Assertions.assertEquals(3, clients.size(), "MBeanClient has been mocked");
        Assertions.assertEquals(1, services.size(), "Executor has been mocked");

        // close it
        collector.close();

        // client & service are closed
        Mockito.verify(clients.get(0), Mockito.times(1)).close();
        Mockito.verify(clients.get(1), Mockito.times(1)).close();
        Mockito.verify(clients.get(2), Mockito.times(1)).close();
        Assertions.assertTrue(services.get(0).isShutdown());
      }
    }
  }

  @Test
  void testFetcherErrorHandling() {
    AtomicBoolean called = new AtomicBoolean();
    Fetcher noSuchFetcher =
        (client) -> {
          BeanObject beanObject =
              client.queryBean(
                  BeanQuery.builder().domainName("no.such.metric").property("k", "v").build());
          return List.of(() -> beanObject);
        };
    try (var collector = MetricCollector.builder().interval(Duration.ofMillis(100)).build()) {
      collector.addFetcher(
          noSuchFetcher,
          (id, ex) -> {
            Assertions.assertEquals(-1, id);
            Assertions.assertInstanceOf(NoSuchElementException.class, ex);
            called.set(true);
          });
      collector.registerLocalJmx(-1);

      Utils.sleep(Duration.ofMillis(300));
      Assertions.assertTrue(called.get(), "The error was triggered");
    }
  }

  @Test
  void testSamplingErrorHandling() {
    AtomicBoolean called = new AtomicBoolean();
    RuntimeException theError = Mockito.spy(new RuntimeException("Boom!"));
    Fetcher expFetcher =
        (client) -> {
          called.set(true);
          throw theError;
        };
    try (var collector =
        MetricCollector.builder().threads(1).interval(Duration.ofMillis(100)).build()) {
      collector.addFetcher(expFetcher);
      collector.registerLocalJmx(-1);

      Utils.sleep(Duration.ofMillis(500));
      Assertions.assertTrue(called.get(), "The error occurred");
      Mockito.verify(theError, Mockito.atLeast(2)).printStackTrace();
    }
  }

  @Test
  void testCleaner() {
    try (var collector =
        MetricCollector.builder()
            .expiration(Duration.ofMillis(2000))
            .cleanerInterval(Duration.ofMillis(50))
            .interval(Duration.ofMillis(100))
            .build()) {
      collector.addFetcher(memoryFetcher);
      collector.registerLocalJmx(0);

      Utils.sleep(Duration.ofMillis(1500));
      var beforeCleaning = collector.metrics(JvmMemory.class, 0, 0).collect(Collectors.toList());
      Assertions.assertFalse(beforeCleaning.isEmpty(), "There are some metrics");
      Utils.sleep(Duration.ofMillis(1500));
      var afterCleaning = collector.metrics(JvmMemory.class, 0, 0).collect(Collectors.toList());

      Assertions.assertTrue(
          afterCleaning.get(0).createdTimestamp() != beforeCleaning.get(0).createdTimestamp(),
          "different old metric: "
              + afterCleaning.get(0).createdTimestamp()
              + " != "
              + beforeCleaning.get(0).createdTimestamp());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Answer<T> sniff(String functionName, Collection<T> collector) {
    return (invocation) -> {
      if (invocation.getMethod().getName().equals(functionName)) {
        Object o = Mockito.spy(invocation.callRealMethod());
        collector.add((T) o);
        return (T) o;
      } else {
        return (T) invocation.callRealMethod();
      }
    };
  }
}
