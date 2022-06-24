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
package org.astraea.app.balancer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.balancer.executor.RebalanceExecutionContext;
import org.astraea.app.balancer.executor.RebalanceExecutionResult;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BalancerConfigsTest {

  private static class TestData {
    public static final String valueJmx =
        "1001@service:jmx:rmi://host1:5566/jndi/rmi://host1:5566/jmxrmi,"
            + "1002@service:jmx:rmi://host2:5566/jndi/rmi://host2:5566/jmxrmi,"
            + "1003@service:jmx:rmi://host3:5566/jndi/rmi://host3:5566/jmxrmi";
    public static final Map<Integer, JMXServiceURL> expJmx =
        Utils.packException(
            () ->
                Map.of(
                    1001,
                        new JMXServiceURL(
                            "service:jmx:rmi://host1:5566/jndi/rmi://host1:5566/jmxrmi"),
                    1002,
                        new JMXServiceURL(
                            "service:jmx:rmi://host2:5566/jndi/rmi://host2:5566/jmxrmi"),
                    1003,
                        new JMXServiceURL(
                            "service:jmx:rmi://host3:5566/jndi/rmi://host3:5566/jmxrmi")));

    public static final String valueCostFunctions =
        String.join(",", DummyCostFunction.class.getName(), DummyCostFunction.class.getName());
    public static final List<Class<?>> expCostFunctions =
        List.of(DummyCostFunction.class, DummyCostFunction.class);

    public static final String valueGenerator = DummyGenerator.class.getName();
    public static final Class<?> expGenerator = DummyGenerator.class;

    public static final String valueExecutor = DummyExecutor.class.getName();
    public static final Class<?> expExecutor = DummyExecutor.class;

    static class DummyCostFunction implements CostFunction {
      @Override
      public Fetcher fetcher() {
        throw new UnsupportedOperationException();
      }
    }

    static class DummyGenerator implements RebalancePlanGenerator {
      @Override
      public Stream<RebalancePlanProposal> generate(
          ClusterInfo clusterInfo, ClusterLogAllocation baseAllocation) {
        throw new UnsupportedOperationException();
      }
    }

    static class DummyExecutor implements RebalancePlanExecutor {
      @Override
      public RebalanceExecutionResult run(RebalanceExecutionContext executionContext) {
        throw new UnsupportedOperationException();
      }
    }
  }

  static <T> Arguments passCase(String key, String value, T expectedReturn) {
    return Arguments.arguments(true, key, value, expectedReturn);
  }

  static <T> Arguments failCase(String key, String value) {
    return Arguments.arguments(false, key, value, null);
  }

  static Stream<Arguments> testcases() {
    return Stream.of(
        passCase(BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker0:5566", "broker0:5566"),
        passCase(BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:5566", "broker1:5566"),
        passCase(BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker2:5566", "broker2:5566"),
        passCase(BalancerConfigs.JMX_SERVERS_CONFIG, TestData.valueJmx, TestData.expJmx),
        failCase(BalancerConfigs.JMX_SERVERS_CONFIG, "1001@localhost:5566"),
        passCase(BalancerConfigs.METRICS_SCRAPING_QUEUE_SIZE_CONFIG, "10", 10),
        failCase(BalancerConfigs.METRICS_SCRAPING_QUEUE_SIZE_CONFIG, "ten"),
        passCase(BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "10", Duration.ofMillis(10)),
        failCase(BalancerConfigs.METRICS_SCRAPING_INTERVAL_MS_CONFIG, "50cent"),
        passCase(BalancerConfigs.METRIC_WARM_UP_COUNT_CONFIG, "10", 10),
        failCase(BalancerConfigs.METRIC_WARM_UP_COUNT_CONFIG, "lOO"),
        passCase(BalancerConfigs.BALANCER_IGNORED_TOPICS_CONFIG, "topic", Set.of("topic")),
        passCase(BalancerConfigs.BALANCER_IGNORED_TOPICS_CONFIG, "a,b,c", Set.of("a", "b", "c")),
        passCase(
            BalancerConfigs.BALANCER_COST_FUNCTIONS,
            TestData.valueCostFunctions,
            TestData.expCostFunctions),
        failCase(BalancerConfigs.BALANCER_COST_FUNCTIONS, Object.class.getName()),
        passCase(
            BalancerConfigs.BALANCER_REBALANCE_PLAN_GENERATOR,
            TestData.valueGenerator,
            TestData.expGenerator),
        failCase(BalancerConfigs.BALANCER_REBALANCE_PLAN_GENERATOR, Object.class.getName()),
        passCase(
            BalancerConfigs.BALANCER_REBALANCE_PLAN_EXECUTOR,
            TestData.valueExecutor,
            TestData.expExecutor),
        failCase(BalancerConfigs.BALANCER_REBALANCE_PLAN_EXECUTOR, Object.class.getName()),
        passCase(BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION, "3000", 3000),
        failCase(BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION, "owo"));
  }

  @ParameterizedTest
  @MethodSource(value = "testcases")
  void testConfig(boolean shouldPass, String key, String value, Object expectedReturn) {
    var config = Configuration.of(Map.of(key, value));
    var balancerConfig = new BalancerConfigs(config, false);
    Supplier<?> fetchConfig =
        () ->
            Utils.packException(
                () ->
                    Arrays.stream(BalancerConfigs.class.getMethods())
                        .filter(m -> m.getAnnotation(BalancerConfigs.Config.class) != null)
                        .filter(
                            m -> m.getAnnotation(BalancerConfigs.Config.class).key().equals(key))
                        .findFirst()
                        .orElseThrow()
                        .invoke(balancerConfig));

    if (shouldPass) {
      Assertions.assertEquals(expectedReturn, fetchConfig.get());
    } else {
      Assertions.assertThrows(Throwable.class, fetchConfig::get);
    }
  }

  public static BalancerConfigs noCheckConfig(Map<String, String> config) {
    return new BalancerConfigs(Configuration.of(config), false);
  }
}
