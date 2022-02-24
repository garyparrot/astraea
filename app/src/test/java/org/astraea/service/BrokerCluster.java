package org.astraea.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.server.KafkaServer;

public interface BrokerCluster extends AutoCloseable {

  /** @return brokers information. the form is "host_a:port_a,host_b:port_b" */
  String bootstrapServers();

  /** @return the log folders used by each broker */
  Map<Integer, Set<String>> logFolders();

  /** @return the actual KafkaServer instances */
  List<KafkaServer> servers();
}
