package org.astraea.balancer.alpha;

import static org.astraea.balancer.alpha.BalancerUtils.clusterSnapShot;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.argument.Field;
import org.astraea.balancer.alpha.generator.MonkeyPlanGenerator;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.CostFunction;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.topic.TopicAdmin;

public class Balancer implements Runnable {

  private final Argument argument;
  private final Thread balancerThread;
  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final MetricCollector metricCollector;
  private final Set<CostFunction> registeredCostFunction;
  private final Map<CostFunction, Fetcher> registeredFetchers;
  private final ScheduledExecutorService scheduledExecutorService;
  private final RebalancePlanGenerator<Void> rebalancePlanGenerator;
  private final TopicAdmin topicAdmin;

  public Balancer(Argument argument) {
    // initialize member variables
    this.argument = argument;
    this.jmxServiceURLMap = argument.jmxServiceURLMap;
    this.registeredCostFunction = Set.of(CostFunction.throughput());
    this.registeredFetchers =
        registeredCostFunction.stream()
            .collect(Collectors.toUnmodifiableMap(Function.identity(), CostFunction::fetcher));
    this.scheduledExecutorService = Executors.newScheduledThreadPool(8);

    // initialize main component
    this.balancerThread = new Thread(this);
    this.metricCollector =
        new MetricCollector(
            this.jmxServiceURLMap, this.registeredFetchers.values(), this.scheduledExecutorService);
    this.topicAdmin = TopicAdmin.of(argument.props());
    // TODO: implement better plan generation
    this.rebalancePlanGenerator = new MonkeyPlanGenerator(this.topicAdmin);
  }

  public void start() {
    balancerThread.start();
  }

  public void run() {
    this.metricCollector.start();

    // schedule a check for a period of time
    final long periodMs = Duration.ofMinutes(1).toMillis();
    while (!Thread.interrupted()) {
      // generate cluster info
      final var clusterInfo =
          ClusterInfo.of(clusterSnapShot(topicAdmin), metricCollector.fetchMetrics());

      // dump metrics into cost function
      Map<CostFunction, Map<Integer, Double>> brokerScores =
          registeredCostFunction.parallelStream()
              .map(costFunction -> Map.entry(costFunction, costFunction.cost(clusterInfo)))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

      // print out current score
      BalancerUtils.printCostFunction(brokerScores);

      if (isClusterImbalance()) {
        final var proposal = rebalancePlanGenerator.generate(clusterInfo, null);

        // describe the proposal
        BalancerUtils.describeProposal(
            proposal, BalancerUtils.currentAllocation(topicAdmin, clusterInfo));
      }
      try {
        TimeUnit.MILLISECONDS.sleep(periodMs);
      } catch (InterruptedException e) {
        e.printStackTrace();
        break;
      }
    }
  }

  private boolean isClusterImbalance() {
    // TODO: Implement this
    return true;
  }

  public void stop() {
    this.metricCollector.close();
    this.scheduledExecutorService.shutdownNow();
  }

  public static void main(String[] args) throws InterruptedException {
    final Argument argument = org.astraea.argument.Argument.parse(new Argument(), args);
    final Balancer balancer = new Balancer(argument);
    balancer.start();
    balancer.balancerThread.join();
    balancer.stop();
  }

  static class Argument extends org.astraea.argument.Argument {

    @Parameter(
        names = {"--jmx.server.file"},
        description =
            "Path to a java properties file that contains all the jmxServiceUrl definitions and their corresponding broker.id",
        converter = JmxServiceUrlMappingFileField.class,
        required = true)
    Map<Integer, JMXServiceURL> jmxServiceURLMap;

    public static class JmxServiceUrlMappingFileField extends Field<Map<Integer, JMXServiceURL>> {

      static final Pattern serviceUrlKeyPattern =
          Pattern.compile("broker\\.(?<brokerId>[1-9][0-9]{0,9})");

      static Map.Entry<Integer, JMXServiceURL> transformEntry(Map.Entry<String, String> entry) {
        final Matcher matcher = serviceUrlKeyPattern.matcher(entry.getKey());
        if (matcher.matches()) {
          try {
            int brokerId = Integer.parseInt(matcher.group("brokerId"));
            final JMXServiceURL jmxServiceURL = new JMXServiceURL(entry.getValue());
            return Map.entry(brokerId, jmxServiceURL);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad integer format for " + entry.getKey(), e);
          } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                "Bad JmxServiceURL format for " + entry.getValue(), e);
          }
        } else {
          throw new IllegalArgumentException(
              "Bad key format for "
                  + entry.getKey()
                  + " no match for the following format :"
                  + serviceUrlKeyPattern.pattern());
        }
      }

      @Override
      public Map<Integer, JMXServiceURL> convert(String value) {
        final Properties properties = new Properties();

        try (var reader = Files.newBufferedReader(Path.of(value))) {
          properties.load(reader);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        return properties.entrySet().stream()
            .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
            .map(
                entry -> {
                  try {
                    return transformEntry(entry);
                  } catch (Exception e) {
                    throw new IllegalArgumentException(
                        "Failed to process JMX service URL map:" + value, e);
                  }
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    }
  }
}
