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
package org.astraea.app.loading;

import com.beust.jcommander.Parameter;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.astraea.app.argument.Argument;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;

public class ConsumeLoading extends Argument {

  @Parameter(
      names = {"--topic"},
      required = true)
  String topicName;

  @Parameter(names = {"--fanout"})
  int fanout = 1;

  public static void main(String[] args) throws Exception {
    ConsumeLoading parse = Argument.parse(new ConsumeLoading(), args);
    parse.run();
  }

  public void run() throws InterruptedException {
    System.out.println("Target: " + topicName);
    System.out.println("Fanout: " + fanout);
    Supplier<KafkaConsumer<Bytes, Bytes>> nextConsumer =
        () -> {
          var randomGroupName = Utils.randomString();
          KafkaConsumer<Bytes, Bytes> consumer =
              new KafkaConsumer<>(
                  Map.of(
                      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      bootstrapServers(),
                      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      BytesDeserializer.class,
                      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      BytesDeserializer.class,
                      ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                      false,
                      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                      false,
                      ConsumerConfig.CHECK_CRCS_CONFIG,
                      false,
                      ConsumerConfig.GROUP_ID_CONFIG,
                      randomGroupName));
          consumer.subscribe(Set.of(topicName));
          return consumer;
        };
    var resultQueue = new ConcurrentLinkedQueue<ConsumerRecords<Bytes, Bytes>>();
    var valueSizeCounter = new LongAdder();
    Consumer<KafkaConsumer<Bytes, Bytes>> consumeTask =
        (consumer) -> {
          while (!Thread.currentThread().isInterrupted()) {
            for (int i = 0; i < 10; i++) {
              // polling data out
              ConsumerRecords<Bytes, Bytes> poll = consumer.poll(Duration.ofMillis(100));
              resultQueue.add(poll);
              // commit progress
              consumer.commitAsync();
            }
          }
        };
    Runnable counting =
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<Bytes, Bytes> poll = resultQueue.poll();
            if (poll != null) poll.forEach(i -> valueSizeCounter.add(i.serializedValueSize()));
          }
        };

    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(16);
    ExecutorService executorService = Executors.newCachedThreadPool();

    IntStream.range(0, fanout)
        .mapToObj(i -> nextConsumer.get())
        .forEach(k -> executorService.execute(() -> consumeTask.accept(k)));

    executorService.execute(counting);
    executorService.execute(counting);
    executorService.execute(counting);

    scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          System.out.println("Peek queue size: " + resultQueue.size());
          long l = valueSizeCounter.sumThenReset();
          System.out.println(DataUnit.Byte.of(l) + " uncompressed value read");
        },
        1,
        1,
        TimeUnit.SECONDS);

    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    scheduledExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }
}
