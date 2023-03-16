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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.astraea.common.DataRate;
import org.astraea.common.Utils;

public class DedicateConsumer {

  public static final String realCluster =
      "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655,192.168.103.181:25655,192.168.103.182:25655";

  public static void main(String[] args) {
    var bootstrap = args[0];
    var topic = args[1];
    var partition = Integer.parseInt(args[2]);
    System.out.println("Bootstrap: " + bootstrap);
    System.out.println("Subscribe Target: " + topic);

    try (var consumer =
        new KafkaConsumer<>(
            Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class),
                Map.entry(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 4024000),
                Map.entry(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760),
                Map.entry(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 10485760),
                Map.entry(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 100),
                Map.entry(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 0),
                // Map.entry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500),
                // Map.entry(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 5048576),
                Map.entry(ConsumerConfig.CHECK_CRCS_CONFIG, false)))) {
      // consumer.assign(Set.of(new TopicPartition(topic, partition)));
      consumer.assign(consumer.partitionsFor(topic)
          .stream()
          .map(x -> new TopicPartition(x.topic(), x.partition()))
          .collect(Collectors.toUnmodifiableList()));

      var counter = new LongAdder();
      var consumerConsumedRate =
          consumer.metrics().keySet().stream()
              .filter(name -> name.name().equals("bytes-consumed-rate"))
              .filter(name -> !name.tags().containsKey("topic"))
              .findFirst()
              .orElseThrow();

      CompletableFuture.runAsync(
          () -> {
            while (!Thread.currentThread().isInterrupted()) {
              Utils.sleep(Duration.ofSeconds(1));
              var consumeRate = ((Double) consumer.metrics().get(consumerConsumedRate).metricValue()).longValue();
              System.out.println("Consume Rate: " + DataRate.Byte.of(consumeRate).perSecond());
              System.out.println("Record Consumed: " + counter.longValue());
              consumer.metrics().values()
                      .stream()
                      .filter(name -> name.metricName().name().equals("records-lag"))
                      .forEach(metric -> {
                        var tag = metric.metricName().tags();
                        var topicName = tag.get("topic");
                        var partitionIndex = tag.get("partition");
                        var lag = ((Double) metric.metricValue());
                        System.out.printf("Lag for \"%s-%s\": %f%n", topicName, partitionIndex, lag);
                      });
            }
          })
          .whenComplete((i, err) -> {
            if(err != null)
              err.printStackTrace();
          });

      for(int i = 0; i < 6; i++) {
          while (!Thread.currentThread().isInterrupted()) {
            var a = consumer.poll(Duration.ofSeconds(1));
            counter.add(a.count());
          }
      }
    }
  }
}
