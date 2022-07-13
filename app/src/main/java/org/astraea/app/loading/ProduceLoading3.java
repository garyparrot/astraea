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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.argument.Argument;
import org.astraea.app.argument.Field;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;

public class ProduceLoading3 extends Argument {

  @Parameter(names = "--topic")
  public String topicName = Utils.randomString();

  @Parameter(names = "--producers", description = "-1 for auto")
  public int producers = -1;

  @Parameter(names = "--record.size", converter = DataSize.Field.class)
  public DataSize recordSize = DataUnit.KiB.of(10);

  @Parameter(names = "--throttle", converter = DataSize.Field.class)
  public DataSize throttle = DataUnit.MiB.of(100);

  @Parameter(names = "--max.in.flight")
  public int maxInFlight = 30;

  @Parameter(names = "--batch.size", converter = DataSize.Field.class)
  public DataSize batchSize = DataUnit.MiB.of(10);

  @Parameter(names = "--linger.ms")
  public int lingerMs = 1000;

  @Parameter(names = "--load.fraction", converter = LoadFractionConvertor.class)
  public Map<TopicPartition, DataSize> loadMap = Map.of();

  @Parameter(names = "--buffer.memory", converter = DataSize.Field.class)
  public DataSize bufferMemory = DataUnit.MiB.of(30);

  @Parameter(names = "--send.limit")
  public int sendLimit = 1_000_000;

  @Parameter(names = "--random-sine-wave-count")
  public int sineWaveCount = 0;

  static class LoadFractionConvertor extends Field<Map<TopicPartition, DataSize>> {
    @Override
    public Map<TopicPartition, DataSize> convert(String value) {
      return Arrays.stream(value.split(","))
          .map(x -> x.split("="))
          .map(x -> Map.entry(x[0], x[1]))
          .collect(
              Collectors.toMap(
                  x -> TopicPartition.of(x.getKey()),
                  x -> new DataSize.Field().convert(x.getValue())));
    }
  }

  public static void main(String[] args) throws Exception {
    ProduceLoading3 app = Argument.parse(new ProduceLoading3(), args);
    app.run();
  }

  public void run() throws InterruptedException {
    long recordBytes = recordSize.measurement(DataUnit.Byte).longValue();
    long throttleBytes = throttle.measurement(DataUnit.Byte).longValue();
    if (!loadMap.isEmpty())
      throttleBytes =
          loadMap.values().stream()
              .reduce(DataUnit.Byte.of(0), DataSize::add)
              .measurement(DataUnit.Byte)
              .longValue();
    System.out.printf("Throttle: %s%n", DataUnit.Byte.of(throttleBytes));
    System.out.printf("Record size: %s%n", recordSize);

    // auto produces, for every 100MiB, add one producer
    if (producers < 0) producers = (int) (throttleBytes / 100 / 1024 / 1024) + 2;
    System.out.println("Launch " + producers + " producers.");

    System.out.println("Prepare producer");

    var theValue = new byte[(int) recordBytes];
    var sendLimitMs = sendLimit;
    Function<TopicPartition, ProducerRecord<Bytes, Bytes>> nextRecord2 =
        (tp) ->
            new ProducerRecord<>(
                tp.topic(),
                tp.partition(),
                System.nanoTime() + sendLimitMs,
                null,
                Bytes.wrap(theValue));

    // spread the tasks by data size
    var dutyList =
        IntStream.range(0, producers)
            .boxed()
            .collect(Collectors.toMap(x -> x, x -> new HashMap<TopicPartition, DataSize>()));
    loadMap.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .forEach(
            entry -> {
              var easyBro =
                  dutyList.entrySet().stream()
                      .min(
                          Comparator.comparing(
                              ee ->
                                  ee.getValue().values().stream()
                                      .reduce(DataUnit.Byte.of(0), DataSize::add)))
                      .orElseThrow();
              easyBro.getValue().put(entry.getKey(), entry.getValue());
            });

    ExecutorService executor = Executors.newCachedThreadPool();
    IntStream.range(0, producers)
        .forEach(
            i -> {
              var producer = producer();
              var task = new SendingTask(producer, recordSize, dutyList.get(i));
              executor.submit(task);
            });

    //noinspection ResultOfMethodCallIgnored
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }

  public static ProducerRecord<Bytes, Bytes> aRecord(TopicPartition topicPartition, byte[] values) {
    return new ProducerRecord<>(
        topicPartition.topic(), topicPartition.partition(), null, Bytes.wrap(values));
  }

  public KafkaProducer<Bytes, Bytes> producer() {
    return new KafkaProducer<>(
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            BytesSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            BytesSerializer.class,
            ProducerConfig.LINGER_MS_CONFIG,
            lingerMs,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            maxInFlight,
            ProducerConfig.ACKS_CONFIG,
            "0",
            ProducerConfig.BUFFER_MEMORY_CONFIG,
            bufferMemory.measurement(DataUnit.Byte).longValue(),
            ProducerConfig.BATCH_SIZE_CONFIG,
            batchSize.measurement(DataUnit.Byte).intValue()));
  }

  public static Function<Double, Double> nextRandomComposedWave(int waveCount, Random random) {
    if (waveCount == 0) return (x) -> 1.0;

    final var coefficients =
        IntStream.range(0, waveCount)
            .mapToDouble(i -> random.nextInt(40) * random.nextDouble())
            .toArray();
    final var offsets =
        IntStream.range(0, waveCount)
            .mapToDouble(i -> random.nextInt(40) * random.nextDouble())
            .toArray();

    return (v) ->
        IntStream.range(0, waveCount)
                .mapToDouble(i -> coefficients[i] * v + offsets[i])
                .map(Math::sin)
                .average()
                .orElseThrow()
            + 1;
  }

  public static class SendingTask implements Runnable {

    private final KafkaProducer<Bytes, Bytes> producer;
    private final DataSize recordSize;
    private final Map<TopicPartition, DataSize> dataPerSecond;
    private final byte[] standardRecord;
    private final Function<Double, Double> wave;

    public SendingTask(
        KafkaProducer<Bytes, Bytes> producer,
        DataSize recordSize,
        Map<TopicPartition, DataSize> dataPerSecond,
        Function<Double, Double> wave) {
      this.producer = producer;
      this.standardRecord = new byte[recordSize.measurement(DataUnit.Byte).intValue()];
      this.recordSize = recordSize;
      this.dataPerSecond = dataPerSecond;
      this.wave = wave;
    }

    @Override
    public void run() {
      var totalSize = dataPerSecond.values().stream().reduce(DataUnit.Byte.of(0), DataSize::add);
      var records =
          dataPerSecond.entrySet().stream()
              .flatMap(
                  entry -> {
                    var topicPartition = entry.getKey();
                    var dataSizeByte = entry.getValue().measurement(DataUnit.Byte).longValueExact();
                    var recordByte = recordSize.measurement(DataUnit.Byte).longValueExact();
                    var recordCount = dataSizeByte / recordByte;
                    var theRecords = aRecord(topicPartition, standardRecord);
                    var remaining = dataSizeByte - recordCount * recordByte;
                    var remainingRecord =
                        remaining != 0 ? aRecord(topicPartition, new byte[(int) remaining]) : null;

                    return Stream.concat(
                        Stream.generate(() -> theRecords).limit(recordCount),
                        Stream.ofNullable(remainingRecord));
                  })
              .sorted(Comparator.comparing(Object::hashCode))
              .collect(Collectors.toUnmodifiableList());

      double now = 0;
      while (!Thread.currentThread().isInterrupted()) {
        // for every second, iterate all the records
        int counter = 0;
        long s = System.nanoTime();
        for (var record : records) {
          if (ThreadLocalRandom.current().nextInt(0, 100) == 0) {
            long i = System.nanoTime();
            if ((i - s) / 1_000_000 > 1000) {
              System.out.printf(
                  "Interrupt task, %d sent, %d dropped.%n", counter, records.size() - counter);
              break;
            }
          }
          var intensifier = wave.apply(now);
          now += 0.001;
          counter++;
          if (intensifier == 1.0) {
            producer.send(record);
          } else if (intensifier > 1.0) {
            producer.send(record);
            producer.send(
                aRecord(
                    new TopicPartition(record.topic(), record.partition()),
                    new byte[(int) (record.value().get().length * (intensifier - 1))]));
          } else if (intensifier < 1.0) {
            producer.send(
                aRecord(
                    new TopicPartition(record.topic(), record.partition()),
                    new byte[(int) (record.value().get().length * (intensifier))]));
          }
        }
        long t = System.nanoTime();
        long passedMs = (t - s) / 1_000_000;
        if (passedMs > 1000)
          System.out.printf(
              "[Slow Warning] sent %d records(%s) in %d ms.%n",
              records.size(), totalSize, passedMs);
        else
          System.out.printf(
              "[Ok] sent %d records(%s) in %d ms.%n", records.size(), totalSize, passedMs);
        long t2 = System.nanoTime();
        long passedMs2 = (t2 - s) / 1_000_000;
        Utils.sleep(Duration.ofMillis(1000 - passedMs2));
      }

      producer.close();
    }
  }
}
