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
package org.astraea.balancer.alpha;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.jmx.MBeanClient;

/** Doing metric collector for balancer. */
public class MetricCollector implements AutoCloseable {

  private final Duration fetchInterval;
  /**
   * The number of old time series to keep in data structure. note that this is not a strict upper
   * limit. The actual size might exceed. This issue is minor and fixing that might cause
   * performance issue. So no. This number must be larger than zero.
   */
  // TODO: guess what? this value must be bigger once there are many topic/partition/broker. A big
  // thanks to the design of ClusterInfo.
  private final int timeSeriesKeeps = 1000;

  private final Map<Integer, MBeanClient> mBeanClientMap;
  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final Fetcher aggregatedFetcher;
  private final Map<Integer, ConcurrentLinkedQueue<HasBeanObject>> metricTimeSeries;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean started = new AtomicBoolean();
  private final List<ScheduledFuture<?>> scheduledFutures = new LinkedList<>();
  private final AtomicLong fetchCounter = new AtomicLong();

  /**
   * BalancerMetricCollector
   *
   * @param jmxServiceURLMap the map of brokerId and corresponding JmxUrl
   * @param fetchers the fetcher of interested metrics
   * @param scheduledExecutorService the executor service for schedule tasks, <strong>DO NOT
   *     SHUTDOWN THIS</strong>.
   */
  public MetricCollector(
      Map<Integer, JMXServiceURL> jmxServiceURLMap,
      Collection<Fetcher> fetchers,
      ScheduledExecutorService scheduledExecutorService,
      Balancer.Argument argument) {
    this.jmxServiceURLMap = Map.copyOf(jmxServiceURLMap);
    this.aggregatedFetcher = Fetcher.of(fetchers);
    this.mBeanClientMap = new ConcurrentHashMap<>();
    this.metricTimeSeries = new ConcurrentHashMap<>();
    this.executor = scheduledExecutorService;
    this.fetchInterval = argument.metricScrapingInterval;
  }

  public synchronized void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("This metric collector already started");
    }

    jmxServiceURLMap.forEach(
        (brokerId, serviceUrl) -> mBeanClientMap.put(brokerId, MBeanClient.of(serviceUrl)));

    Consumer<Integer> task =
        (brokerId) -> {
          // the following code section perform multiple modification on this data structure without
          // atomic guarantee. this is done by the thread confinement technique. So for any time
          // moment, only one thread can be the writer to this data structure.
          metricTimeSeries
              .computeIfAbsent(brokerId, (ignore) -> new ConcurrentLinkedQueue<>())
              .addAll(aggregatedFetcher.fetch(mBeanClientMap.get(brokerId)));
          while (metricTimeSeries.get(brokerId).size() > timeSeriesKeeps)
            metricTimeSeries.get(brokerId).poll();
        };

    // schedule the fetching process for every broker.
    var futures =
        mBeanClientMap.keySet().stream()
            .map(
                brokerId ->
                    executor.scheduleAtFixedRate(
                        () -> task.accept(brokerId),
                        0,
                        fetchInterval.toMillis(),
                        TimeUnit.MILLISECONDS))
            .collect(Collectors.toUnmodifiableList());
    scheduledFutures.addAll(futures);
    scheduledFutures.add(
        executor.scheduleAtFixedRate(
            fetchCounter::incrementAndGet, 10, fetchInterval.toMillis(), TimeUnit.MILLISECONDS));
  }

  public long fetchCount() {
    return fetchCounter.get();
  }

  /**
   * fetch metrics from the specific brokers. This method is thread safe.
   *
   * @param brokerId the broker id
   * @return a list of requested metrics.
   */
  public synchronized List<HasBeanObject> fetchBrokerMetrics(Integer brokerId) {
    // concurrent data structure + thread confinement to one writer + immutable objects
    if (!started.get()) throw new IllegalStateException("This MetricCollector haven't started");
    return List.copyOf(
        metricTimeSeries.computeIfAbsent(brokerId, (ignore) -> new ConcurrentLinkedQueue<>()));
  }

  public synchronized Map<Integer, Collection<HasBeanObject>> fetchMetrics() {
    return this.jmxServiceURLMap.keySet().stream()
        .collect(Collectors.toUnmodifiableMap(Function.identity(), this::fetchBrokerMetrics));
  }

  @Override
  public synchronized void close() {
    if (!started.get()) return;
    started.set(false);
    scheduledFutures.forEach(x -> x.cancel(true));
    scheduledFutures.forEach(x -> Utils.waitFor(x::isCancelled));
    mBeanClientMap.values().forEach(MBeanClient::close);
    mBeanClientMap.clear();
    metricTimeSeries.clear();
  }
}
