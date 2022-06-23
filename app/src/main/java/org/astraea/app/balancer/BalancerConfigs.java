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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.partitioner.Configuration;

public class BalancerConfigs implements Configuration {

  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  public static final String JMX_SERVERS_CONFIG = "jmx.servers";
  public static final String METRICS_SCRAPING_QUEUE_SIZE_CONFIG = "metrics.scraping.queue.size";
  public static final String METRICS_SCRAPING_INTERVAL_MS_CONFIG = "metrics.scraping.interval.ms";
  public static final String METRIC_WARM_UP_PERCENT_CONFIG = "metrics.warm.up.percent";
  public static final String BALANCER_IGNORED_TOPICS_CONFIG = "balancer.ignored.topics";
  public static final String BALANCER_COST_FUNCTIONS = "balancer.cost.functions";
  public static final String BALANCER_REBALANCE_PLAN_GENERATOR =
      "balancer.rebalance.plan.generator";
  public static final String BALANCER_REBALANCE_PLAN_EXECUTOR = "balancer.rebalance.plan.executor";
  public static final String BALANCER_PLAN_SEARCHING_ITERATION =
      "balancer.plan.searching.iteration";

  private final Configuration configuration;

  public BalancerConfigs(Configuration configuration) {
    this.configuration = configuration;
    sanityCheck();
  }

  /**
   * Ensure the configuration is offered correctly. This method is called on {@link BalancerConfigs}
   * initialization to ensure fail fast.
   */
  private void sanityCheck() {
    List<Method> methods = List.of(this.getClass().getMethods());
    methods.stream()
        .filter(m -> m.getAnnotation(Config.class) != null)
        .filter(m -> m.getAnnotation(Required.class) != null)
        .forEach(m -> requireString(m.getAnnotation(Config.class).key()));
    methods.stream()
        .filter(m -> m.getAnnotation(Config.class) != null)
        .forEach(
            m -> {
              try {
                // TODO: verify config values are in valid range
                Objects.requireNonNull(m.invoke(this));
              } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
              } catch (Exception e) {
                var key = m.getAnnotation(Config.class).key();
                throw new IllegalArgumentException(
                    "Failed to fetch required configuration " + m.getName() + " \"" + key + "\"",
                    e);
              }
            });
  }

  @Config(key = BOOTSTRAP_SERVERS_CONFIG)
  @Required
  public String bootstrapServers() {
    return requireString(BOOTSTRAP_SERVERS_CONFIG);
  }

  @Config(key = JMX_SERVERS_CONFIG)
  @Required
  public Map<Integer, JMXServiceURL> jmxServers() {
    return map(
        JMX_SERVERS_CONFIG,
        ",",
        "@",
        Integer::parseInt,
        (s) -> Utils.packException(() -> new JMXServiceURL(s)));
  }

  @Config(key = METRICS_SCRAPING_QUEUE_SIZE_CONFIG)
  public int metricScrapingQueueSize() {
    return string(METRICS_SCRAPING_QUEUE_SIZE_CONFIG).map(Integer::parseInt).orElse(1000);
  }

  @Config(key = METRICS_SCRAPING_INTERVAL_MS_CONFIG)
  public Duration metricScrapingInterval() {
    return string(METRICS_SCRAPING_INTERVAL_MS_CONFIG)
        .map(Integer::parseInt)
        .map(Duration::ofMillis)
        .orElse(Duration.ofSeconds(1));
  }

  @Config(key = METRIC_WARM_UP_PERCENT_CONFIG)
  public double metricWarmUpPercent() {
    return string(METRIC_WARM_UP_PERCENT_CONFIG).map(Double::parseDouble).orElse(0.5);
  }

  @Config(key = BALANCER_IGNORED_TOPICS_CONFIG)
  public Set<String> ignoredTopics() {
    return string(BALANCER_IGNORED_TOPICS_CONFIG)
        .map(s -> s.split(","))
        .map(Set::of)
        .orElse(Set.of());
  }

  @Config(key = BALANCER_COST_FUNCTIONS)
  @Required
  public List<Class<? extends CostFunction>> costFunctionClasses() {
    return Stream.of(requireString(BALANCER_COST_FUNCTIONS).split(","))
        .map(classname -> resolveClass(classname, CostFunction.class))
        .collect(Collectors.toUnmodifiableList());
  }

  @Config(key = BALANCER_REBALANCE_PLAN_GENERATOR)
  @Required
  public Class<? extends RebalancePlanGenerator> rebalancePlanGeneratorClass() {
    String classname = requireString(BALANCER_REBALANCE_PLAN_GENERATOR);
    return resolveClass(classname, RebalancePlanGenerator.class);
  }

  @Config(key = BALANCER_REBALANCE_PLAN_EXECUTOR)
  @Required
  public Class<? extends RebalancePlanExecutor> rebalancePlanExecutorClass() {
    String classname = requireString(BALANCER_REBALANCE_PLAN_EXECUTOR);
    return resolveClass(classname, RebalancePlanExecutor.class);
  }

  @Config(key = BALANCER_PLAN_SEARCHING_ITERATION)
  @Required
  public int rebalancePlanSearchingIteration() {
    return string(BALANCER_PLAN_SEARCHING_ITERATION).map(Integer::parseInt).orElse(2000);
  }

  private static <T> Class<T> resolveClass(String classname, Class<T> extendedType) {
    Class<?> aClass = Utils.packException(() -> Class.forName(classname));

    if (aClass.isInstance(extendedType)) {
      //noinspection unchecked
      return (Class<T>) aClass;
    } else {
      throw new IllegalArgumentException(
          "The given class \"" + classname + "\" is not a instance of " + extendedType.getName());
    }
  }

  @Override
  public Optional<String> string(String key) {
    return configuration.string(key);
  }

  @Override
  public List<String> list(String key, String separator) {
    return configuration.list(key, separator);
  }

  @Override
  public <K, V> Map<K, V> map(
      String key,
      String listSeparator,
      String mapSeparator,
      Function<String, K> keyConverter,
      Function<String, V> valueConverter) {
    return configuration.map(key, listSeparator, mapSeparator, keyConverter, valueConverter);
  }

  @Override
  public Set<Map.Entry<String, String>> entrySet() {
    return configuration.entrySet();
  }

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface Required {}

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface Config {
    String key();
  }
}
