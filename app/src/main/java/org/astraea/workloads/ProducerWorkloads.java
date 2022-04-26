package org.astraea.workloads;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.astraea.topic.TopicAdmin;
import org.astraea.utils.DataUnit;
import org.astraea.workloads.annotations.NamedArg;

public final class ProducerWorkloads {

  private ProducerWorkloads() {}

  public static Runnable createTopic(
      @NamedArg(name = "bootstrapServer") String bootstrapServer,
      @NamedArg(name = "topicName") String topicName,
      @NamedArg(name = "partitionCount") int partitionCount,
      @NamedArg(name = "replicaCount") short replicaCount) {
    return () -> {
      try (TopicAdmin topicAdmin = TopicAdmin.of(bootstrapServer)) {
        topicAdmin
            .creator()
            .topic(topicName)
            .numberOfPartitions(partitionCount)
            .numberOfReplicas(replicaCount)
            .create();
      }
    };
  }

  public static ProducerWorkload<?, ?> powImbalanceWorkload(
      @NamedArg(name = "bootstrapServer") String bootstrapServer,
      @NamedArg(name = "topicName") String topicName,
      @NamedArg(name = "power") double power,
      @NamedArg(
              name = "ceiling",
              description = "reset the proportion counter once it reach the ceiling")
          double ceiling,
      @NamedArg(name = "recordSize") int recordSize,
      @NamedArg(name = "iterationWaitMs") int iterationWaitMs) {
    return ProducerWorkloadBuilder.builder()
        .bootstrapServer(bootstrapServer)
        .keySerializer(BytesSerializer.class)
        .valueSerializer(BytesSerializer.class)
        .build(
            (producer) -> {
              final Bytes bytes = Bytes.wrap(new byte[recordSize]);
              while (!Thread.currentThread().isInterrupted()) {
                final int partitionSize = producer.partitionsFor(topicName).size();

                // the proportion of each partition is (rate):(rate*2):(rate*3):(rate(3)...
                // if the value exceeded the overflow point, the proportion is reset to 1
                double proportion = 1;
                for (int i = 0; i < partitionSize; i++, proportion *= power) {
                  if (proportion > ceiling) proportion = 1;
                  for (int j = 0; j < proportion; j++) {
                    producer.send(new ProducerRecord<>(topicName, i, null, bytes));
                  }
                }
                try {
                  TimeUnit.MILLISECONDS.sleep(iterationWaitMs);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  break;
                }
              }
            });
  }

  public static ProducerWorkload<?, ?> straightWorkload(
      @NamedArg(name = "bootstrapServer") String bootstrapServer,
      @NamedArg(name = "topicName") String topicName,
      @NamedArg(name = "partition") int partition,
      @NamedArg(name = "recordSize") int recordSize,
      @NamedArg(name = "iterationWaitMs") int iterationWaitMs) {
    return ProducerWorkloadBuilder.builder()
        .bootstrapServer(bootstrapServer)
        .keySerializer(BytesSerializer.class)
        .valueSerializer(BytesSerializer.class)
        .build(
            (producer) -> {
              final Bytes bytes = Bytes.wrap(new byte[recordSize]);
              while (!Thread.currentThread().isInterrupted()) {
                producer.send(new ProducerRecord<>(topicName, partition, null, bytes));
                try {
                  TimeUnit.MILLISECONDS.sleep(iterationWaitMs);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                  break;
                }
              }
            });
  }

  public static ProducerWorkload<?, ?> shitShow(
      @NamedArg(name = "bootstrapServer") String bootstrapServer,
      @NamedArg(name = "topicName") String topicName,
      @NamedArg(name = "startOffset") int startOffset,
      @NamedArg(name = "moveStep") int moveStep,
      @NamedArg(name = "recordSize") int recordSize,
      @NamedArg(name = "chunkSize") int chunkSize,
      @NamedArg(name = "iterationWaitMs") int iterationWaitMs,
      @NamedArg(name = "proportion") String proportionString) {
    // the record size is fixed to 100 bytes.
    // the total sending size is (proportion * recordSize), and all these size is going to
    // be divided into multiple small 100 bytes chunks.
    final var value = new Bytes(new byte[chunkSize]);
    final double[] proportion =
        Arrays.stream(proportionString.split(",")).mapToDouble(Double::parseDouble).toArray();
    final var partitionSendingList =
        IntStream.range(0, proportion.length)
            .mapToObj(index -> Map.entry(startOffset + moveStep * index, proportion[index]))
            .map(p -> Map.entry(p.getKey(), (int) (p.getValue() * recordSize / chunkSize)))
            .peek(
                p ->
                    System.out.printf(
                        "Assign %d chunks for partition %d%n", p.getValue(), p.getKey()))
            .toArray(Map.Entry[]::new);
    System.out.printf(
        "RecordSize %d, IterationWait %d, ProportionAvg %s%n",
        recordSize, iterationWaitMs, Arrays.stream(proportion).average().toString());

    return ProducerWorkloadBuilder.builder()
        .bootstrapServer(bootstrapServer)
        .keySerializer(BytesSerializer.class)
        .valueSerializer(BytesSerializer.class)
        .configs(Map.of(ProducerConfig.ACKS_CONFIG, "0"))
        .configs(Map.of(ProducerConfig.LINGER_MS_CONFIG, 50))
        .configs(Map.of(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 30))
        .configs(
            Map.of(
                ProducerConfig.BATCH_SIZE_CONFIG,
                DataUnit.MB.of(25).measurement(DataUnit.Byte).intValue()))
        .build(
            (producer) -> {
              while (!Thread.currentThread().isInterrupted()) {
                Arrays.stream(partitionSendingList)
                    .parallel()
                    .forEach(
                        partition -> {
                          int partitionId = (int) partition.getKey();
                          int count = (int) partition.getValue();
                          for (int i = 0; i < count; i++) {
                            producer.send(
                                new ProducerRecord<>(topicName, partitionId, null, value));
                          }
                        });
                try {
                  // if we would like to make the bandwidth flow looks nicer, we can for example:
                  // We are sending 1MB proportion per second, now make it 500KB proportion per 0.5
                  // second
                  // This will result in the exactly same bandwidth cost but the flow will look much
                  // smoother.
                  TimeUnit.MILLISECONDS.sleep(iterationWaitMs);
                } catch (InterruptedException e) {
                  break;
                }
              }
            });
  }
}
