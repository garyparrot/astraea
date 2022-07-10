package org.astraea.app.loading;

import com.beust.jcommander.Parameter;
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

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProduceLoading2 extends Argument {

  @Parameter(names =  "--topic")
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

  public Map<TopicPartition, Long> recordPer10Ms = Map.of();

  static class LoadFractionConvertor extends Field<Map<TopicPartition, DataSize>> {
    @Override
    public Map<TopicPartition, DataSize> convert(String value) {
      return Arrays.stream(value.split(","))
          .map(x -> x.split("="))
          .map(x -> Map.entry(x[0], x[1]))
          .collect(Collectors.toMap(
              x -> org.astraea.app.admin.TopicPartition.of(x.getKey()),
              x -> new DataSize.Field().convert(x.getValue())));
    }
  }

  public static void main(String[] args) throws Exception {
    ProduceLoading2 app = Argument.parse(new ProduceLoading2(), args);
    app.run();
  }

  public void run() throws InterruptedException {
    long recordBytes = recordSize.measurement(DataUnit.Byte).longValue();
    long throttleBytes = throttle.measurement(DataUnit.Byte).longValue();
    var recordInterval10Ms = throttleBytes / recordBytes / 100;
    System.out.println("Topic name: " + topicName);
    System.out.printf("Throttle: %s%n", throttle);
    System.out.printf("Record size: %s%n", recordSize);
    System.out.printf("Estimate to send %d records per 100 ms%n", recordInterval10Ms);
    System.out.println("Load fraction: ");

    System.out.println("Prepare producer");
    Supplier<KafkaProducer<Bytes, Bytes>> nextProducer = () -> new KafkaProducer<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class,
        ProducerConfig.LINGER_MS_CONFIG, lingerMs,
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight,
        ProducerConfig.ACKS_CONFIG, "0",
        ProducerConfig.BUFFER_MEMORY_CONFIG, DataUnit.MiB.of(100).measurement(DataUnit.Byte).longValue(),
        ProducerConfig.BATCH_SIZE_CONFIG, batchSize.measurement(DataUnit.Byte).intValue()));

    var theValue = new byte[(int)recordBytes];
    var recordDropped = new LongAdder();
    var sendLimit = 100_000_000;
    Supplier<ProducerRecord<Bytes, Bytes>> nextRecord = () ->
        new ProducerRecord<>(topicName, null, System.nanoTime() + sendLimit, null, Bytes.wrap(theValue));
    Function<TopicPartition, ProducerRecord<Bytes, Bytes>> nextRecord2 = (tp) ->
        new ProducerRecord<>(tp.topic(), tp.partition(), System.nanoTime() + sendLimit, null, Bytes.wrap(theValue));

    var recordQueue = new ConcurrentLinkedDeque<ProducerRecord<Bytes, Bytes>>();
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(16);
    ExecutorService workerPool = Executors.newCachedThreadPool();

    if(!loadMap.isEmpty()) {
      recordPer10Ms = loadMap.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .map(x -> Map.entry(x.getKey(), Math.max(1, x.getValue().measurement(DataUnit.Byte).longValue() / recordBytes)))
          .peek(x -> System.out.printf("%s: %s%n", x.getKey(), x.getValue()))
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              Map.Entry::getValue));
    }

    Runnable submitRecordByLoadingMap = () -> {
      long a = System.nanoTime();
      recordPer10Ms.forEach((tp, records) -> {
        for(int i = 0;i < records; i++)
          recordQueue.add(nextRecord2.apply(tp));
      });
      long b = System.nanoTime();
      long passed = (b - a) / 1_000_000;
      if(passed >= 10)
        System.out.println("Record too slow :p " + passed + " ms");
    };
    Runnable submitRecords = () -> {
      long a = System.nanoTime();
      for(int i = 0; i < recordInterval10Ms; i++)
        recordQueue.add(nextRecord.get());
      long b = System.nanoTime();
      long passed = (b - a) / 1_000_000;
      if(passed >= 10)
        System.out.println("Record too slow :p " + passed + " ms");
    };

    Consumer<KafkaProducer<Bytes, Bytes>> sendRecords = (producer) -> {
      while (!Thread.currentThread().isInterrupted()) {
        for(int i = 0; i < 1000; i++) {
          ProducerRecord<Bytes, Bytes> poll = recordQueue.poll();
          if(poll == null) {
            Utils.sleep(Duration.ofMillis(1));
            continue;
          }
          // stale record
          if(isStaleRecord(poll)) {
            recordDropped.increment();
            continue;
          }
          producer.send(poll);
        }
      }
    };

    // submit records every 100 ms
    if(loadMap.isEmpty())
      executor.scheduleAtFixedRate(submitRecords, 0, 10, TimeUnit.MILLISECONDS);
    else
      executor.scheduleAtFixedRate(submitRecordByLoadingMap, 0, 10, TimeUnit.MILLISECONDS);
    executor.scheduleAtFixedRate(() -> {
      // remove stale records at the front
      while (isStaleRecord(recordQueue.peekFirst())) {
        var poll = recordQueue.pollFirst();
        if(isStaleRecord(poll))
          recordDropped.increment();
        else
          recordQueue.addFirst(poll);
      }
      // show dropped records
      long dropped = recordDropped.sumThenReset();
      System.out.println("Peek queue size: " + recordQueue.size() + ", total " + dropped + " record dropped due to stale.");
    }, 0, 1000, TimeUnit.MILLISECONDS);

    // auto produces, for every 100MiB, add one producer
    if(producers < 0)
      producers = (int)(throttleBytes / 100 / 1024 / 1024) + 2;
    System.out.println("Launch " + producers + " producers.");

    // launch threads to send data
    IntStream.range(0, producers)
        .mapToObj(x -> nextProducer.get())
        .forEach(x -> workerPool.execute(() -> sendRecords.accept(x)));

    // wait
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    workerPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }
  boolean isStaleRecord(ProducerRecord<?, ?> record) {
    return record != null && System.nanoTime() > record.timestamp();
  }

}
