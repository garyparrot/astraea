package org.astraea.app.loading;

import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.astraea.app.argument.Argument;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

public class ProduceLoading extends Argument {

  @Parameter(names =  "--topic")
  public String topicName = Utils.randomString();

  @Parameter(names = "--partitions")
  public int partitionSize = 16;

  @Parameter(names = "--replicas")
  public short replicaSize = 1;

  @Parameter(names = "--record.size", converter = DataSize.Field.class)
  public DataSize recordSize = DataUnit.KiB.of(10);

  @Parameter(names = "--throttle", converter = DataSize.Field.class)
  public DataSize throttle = DataUnit.MiB.of(100);

  public KafkaProducer<Bytes, Bytes> producer;

  public static void main(String[] args) throws Exception {
    ProduceLoading app = Argument.parse(new ProduceLoading(), args);
    app.run();
  }

  public void run() throws InterruptedException {
    long iterationCount = 100;
    long recordBytes = recordSize.measurement(DataUnit.Byte).longValue();
    long throttleBytes = throttle.measurement(DataUnit.Byte).longValue();
    long send10Ms = Math.max(throttleBytes / recordBytes / (1000 / iterationCount), 1);
    System.out.println("Topic name: " + topicName);
    System.out.printf("Throttle: %s%n", throttle);
    System.out.printf("Record size: %s%n", recordSize);
    System.out.printf("Estimate to send %d records per 100 ms%n", send10Ms);

    System.out.println("Prepare producer");
    producer = new KafkaProducer<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class,
        ProducerConfig.LINGER_MS_CONFIG, iterationCount,
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1500,
        ProducerConfig.ACKS_CONFIG, "0",
        ProducerConfig.BUFFER_MEMORY_CONFIG, DataUnit.GiB.of(2).measurement(DataUnit.Byte).longValue(),
        ProducerConfig.BATCH_SIZE_CONFIG, DataUnit.MiB.of(10).measurement(DataUnit.Byte).intValue()));

    var theValue = new byte[(int)recordBytes];
    var theRecord = new ProducerRecord<Bytes, Bytes>(topicName, Bytes.wrap(theValue));
    Supplier<ProducerRecord<Bytes, Bytes>> nextRecord = () -> new ProducerRecord<Bytes, Bytes>(topicName, Bytes.wrap(theValue));

    while(!Thread.currentThread().isInterrupted()) {
      // sending
      var violations = new ArrayList<Long>();
      var violationSum = 0L;

      long a = System.nanoTime();
      for(int i = 0;i < (1000 / iterationCount); i++) {
        long start = System.nanoTime();
        for(int j = 0;j < send10Ms; j++)
          producer.send(nextRecord.get());
        long end = System.nanoTime();
        long msPassed = (end - start) / 1_000_000;
        TimeUnit.MILLISECONDS.sleep(100 - msPassed);
        if(msPassed > 100) {
          violations.add(msPassed);
          violationSum += msPassed;
        }
      }
      producer.flush();
      // checking
      if(!violations.isEmpty())
        System.out.println("Violation occurred " + violations.size() + " times, with " + violationSum / violations.size() + " ms in average.");

      long b = System.nanoTime();
      long pasted = (b - a) / 1_000_000;
      System.out.println("One full iteration done: " + pasted + " ms passed.");
    }
  }

}