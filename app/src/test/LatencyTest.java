public class LatencyTest {

  public static final String realCluster =
      "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655,192.168.103.181:25655,192.168.103.182:25655";

  @Test
  void test10000PartitionCluster() {
    try (var admin = Admin.of(realCluster)) {
      var brokers = admin.brokers().toCompletableFuture().join();
      var jmxPort = 16926;
      var clients = brokers.stream()
          .collect(Collectors.toUnmodifiableMap(
              NodeInfo::id,
              b -> MBeanClient.jndi(b.host(), jmxPort)));
      var cost = HasClusterCost.of(Map.ofEntries(
          Map.entry(new NetworkIngressCost(), 1.0),
          Map.entry(new NetworkEgressCost(), 1.0)));
      try (var store = MetricStore.builder()
          .localReceiver(() -> CompletableFuture.completedStage(clients))
          .sensorsSupplier(() -> Map.of(cost.metricSensor().get(), (a, b) -> {}))
          .beanExpiration(Duration.ofSeconds(30))
          .build()) {
        while (true) {
          var cb = store.clusterBean();
          System.out.println(cb.all().entrySet()
              .stream()
              .collect(Collectors.toUnmodifiableMap(
                  Map.Entry::getKey,
                  x -> x.getValue().size())));
          Utils.sleep(Duration.ofSeconds(1));
        }
      }
    }
  }

}
