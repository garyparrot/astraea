package org.astraea.app.balancer;

import org.astraea.app.cost.ReplicaDiskInCost;
import org.astraea.app.partitioner.Configuration;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BalancerBootstrap {
  public static void main(String[] args) {
    // configs
    var bootstrapServers = "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655,192.168.103.181:25655,192.168.103.182:25655";
    var jmxServers = "0@service:jmx:rmi://192.168.103.177:16926/jndi/rmi://192.168.103.177:16926/jmxrmi,1@service:jmx:rmi://192.168.103.178:16926/jndi/rmi://192.168.103.178:16926/jmxrmi,2@service:jmx:rmi://192.168.103.179:16926/jndi/rmi://192.168.103.179:16926/jmxrmi,3@service:jmx:rmi://192.168.103.180:16926/jndi/rmi://192.168.103.180:16926/jmxrmi,4@service:jmx:rmi://192.168.103.181:16926/jndi/rmi://192.168.103.181:16926/jmxrmi,5@service:jmx:rmi://192.168.103.182:16926/jndi/rmi://192.168.103.182:16926/jmxrmi";
    var costFunctions = Stream.of(
        ReplicaDiskInCost.class)
        .map(Class::getName)
        .collect(Collectors.joining(","));
    var ignoreTopics = "__consumer_offset";
    var configMap = Map.of(
        BalancerConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
        BalancerConfigs.JMX_SERVERS_CONFIG, jmxServers,
        BalancerConfigs.BALANCER_COST_FUNCTIONS, costFunctions,
        BalancerConfigs.BALANCER_IGNORED_TOPICS_CONFIG, ignoreTopics,
        BalancerConfigs.BALANCER_PLAN_SEARCHING_ITERATION, "300");

    // run
    try (Balancer balancer = new Balancer(Configuration.of(configMap))) {
      balancer.run();
    }
  }
}
