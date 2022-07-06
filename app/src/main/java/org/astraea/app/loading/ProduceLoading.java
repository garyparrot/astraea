package org.astraea.app.loading;

import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.astraea.app.argument.Argument;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

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
  public DataSize throttle = DataUnit.MiB.of(5);

  public KafkaProducer<Bytes, Bytes> producer;

  public static void main(String[] args) throws Exception {
    ProduceLoading app = Argument.parse(new ProduceLoading(), args);
    app.run();
  }

  public void run() throws InterruptedException {
    long iterationCount = 100;
    long recordBytes = recordSize.measurement(DataUnit.Byte).longValue();
    long send10Ms = Math.max(throttle.divide(recordBytes).bits().longValue() / 8 / (1000 / iterationCount), 1);
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
        ProducerConfig.BATCH_SIZE_CONFIG, (int)(1.2 * send10Ms * recordBytes)));

    var theValue = new byte[(int)recordBytes];
    var theRecord = new ProducerRecord<Bytes, Bytes>(topicName, null, Bytes.wrap(theValue));

    while(!Thread.currentThread().isInterrupted()) {
      // sending
      var violations = new ArrayList<Long>();
      var violationSum = 0L;

      for(int i = 0;i < (1000 / iterationCount); i++){
        long start = System.nanoTime();
        for(int j = 0;j < send10Ms; j++)
          producer.send(theRecord);
        long end = System.nanoTime();
        long msPassed = (end - start) / 1_000_000;
        TimeUnit.MILLISECONDS.sleep(100 - msPassed);
        if(msPassed > 100) {
          violations.add(msPassed);
          violationSum += msPassed;
        }
      }

      // checking
      if(!violations.isEmpty())
        System.out.println("Violation occurred " + violations.size() + " times, with " + violationSum / violations.size() + " ms in average.");
    }
  }

}