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
package org.astraea.app;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.astraea.common.Utils;

public class DedicateConsumer {

  public static final String RANDOM_GROUP = Utils.randomString();

  public static KafkaConsumer<byte[], byte[]> consumer(String bootstrap) {
    return new KafkaConsumer<>(
        Map.ofEntries(
            Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap),
            Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"),
            Map.entry(ConsumerConfig.GROUP_ID_CONFIG, RANDOM_GROUP),
            Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class),
            Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class),
            Map.entry(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 800000),
            Map.entry(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10400320),
            Map.entry(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 10400320),
            Map.entry(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10400320 / 2),
            Map.entry(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 100),
            Map.entry(ConsumerConfig.CHECK_CRCS_CONFIG, false),
            Map.entry(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 0)));
  }

  public static void main(String[] args) {
    var bootstrap = args[0];
    var topic = args[1];
    var consumerGroupSize = 24;
    System.out.println("Bootstrap: " + bootstrap);
    System.out.println("Subscribe Target: " + topic);

    var kafkaConsumers =
        IntStream.range(0, consumerGroupSize)
            .mapToObj(i -> consumer(bootstrap))
            .peek(consumer -> consumer.subscribe(Set.of(topic)))
            .collect(Collectors.toUnmodifiableList());

    var metrics =
        CompletableFuture.runAsync(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                Utils.sleep(Duration.ofSeconds(1));
                System.out.println("-------------------------------------");
                kafkaConsumers.forEach(
                    consumer ->
                        consumer.metrics().values().stream()
                            .filter(name -> name.metricName().name().equals("records-lag"))
                            .forEach(
                                metric -> {
                                  var tag = metric.metricName().tags();
                                  var topicName = tag.get("topic");
                                  var partitionIndex = tag.get("partition");
                                  var lag = ((Double) metric.metricValue());
                                  System.out.printf(
                                      "Lag for \"%s-%s\": %f%n", topicName, partitionIndex, lag);
                                }));
              }
            });

    var executors = Executors.newFixedThreadPool(consumerGroupSize);
    kafkaConsumers.forEach(
        consumer -> {
          executors.submit(
              () -> {
                while (!Thread.currentThread().isInterrupted()) {
                  consumer.poll(Duration.ofMillis(100));
                }
              });
        });

    metrics.join();
    executors.shutdown();
    kafkaConsumers.forEach(KafkaConsumer::close);
  }
}
