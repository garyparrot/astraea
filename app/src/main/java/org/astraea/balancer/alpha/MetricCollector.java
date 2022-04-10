package org.astraea.balancer.alpha;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.BeanCollector;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.collector.Receiver;
import org.astraea.metrics.jmx.MBeanClient;

/** Doing metric collector for balancer. */
public class MetricCollector implements AutoCloseable {

  private final Duration fetchInterval = Duration.ofSeconds(1);
  private final int timeSeriesKeeps = 5000;

  private final Map<Integer, MBeanClient> mBeanClientMap;
  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final Fetcher aggregatedFetcher;
  private final BeanCollector beanCollector;
  private final Map<Integer, Receiver> metricReceiver;

  /**
   * BalancerMetricCollector
   *
   * @param jmxServiceURLMap the map of brokerId and corresponding JmxUrl
   * @param fetchers the fetcher of interested metrics
   */
  public MetricCollector(
      Map<Integer, JMXServiceURL> jmxServiceURLMap, Collection<Fetcher> fetchers) {
    this.jmxServiceURLMap = Map.copyOf(jmxServiceURLMap);
    this.aggregatedFetcher = Fetcher.of(fetchers);
    this.mBeanClientMap = new ConcurrentHashMap<>();
    this.metricReceiver = new ConcurrentHashMap<>();
    this.beanCollector =
        BeanCollector.builder()
            .clientCreator((ignored, brokerId) -> mBeanClientMap.get(brokerId))
            .interval(fetchInterval)
            .numberOfObjectsPerNode(timeSeriesKeeps)
            .build();

    jmxServiceURLMap.forEach(
        (brokerId, serviceUrl) -> mBeanClientMap.put(brokerId, MBeanClient.of(serviceUrl)));

    jmxServiceURLMap.keySet().stream()
        .map(
            brokerId ->
                Map.entry(
                    brokerId,
                    beanCollector
                        .register()
                        .host("")
                        .port(brokerId)
                        .fetcher(aggregatedFetcher)
                        .build()))
        .forEach(entry -> metricReceiver.put(entry.getKey(), entry.getValue()));
  }

  public Collection<HasBeanObject> fetchBrokerMetrics(Integer brokerId) {
    return metricReceiver.get(brokerId).current();
  }

  public Map<Integer, Collection<HasBeanObject>> fetchMetrics() {
    return this.jmxServiceURLMap.keySet().parallelStream()
        .map(brokerId -> Map.entry(brokerId, this.fetchBrokerMetrics(brokerId)))
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public synchronized void close() {
    metricReceiver.values().forEach(Receiver::close);
    metricReceiver.clear();
    mBeanClientMap.values().forEach(MBeanClient::close);
    mBeanClientMap.clear();
  }
}
