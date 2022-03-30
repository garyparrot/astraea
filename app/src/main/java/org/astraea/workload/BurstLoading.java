package org.astraea.workload;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class BurstLoading {
  public static class Producer implements Workload {
    /**
     * @param argument topic
     * @throws InterruptedException
     */
    @Override
    public void run(String bootstrapServer, String argument) throws InterruptedException {
      final String[] split = argument.split(",");
      var topicName = split[0];
      final KafkaProducer<byte[], byte[]> kafkaProducer =
          org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer();
      var batchSize = 10000;
      var batchSizeLow = 10000; // 1000~1000000
      var batchSizeUp = 1000000;

      var r = 0;
      var speedUpProducer = false;
      var speedDownProducer = false;
      var timeWait = 1000;
      var timeAdd = false;
      while (true) {
        if (!(speedUpProducer || speedDownProducer || timeWait != 1000))
          r = (int) (Math.random() * 200);
        if (r == 1) {
          r = 0;
          speedUpProducer = true;
          System.out.println("speedUpProducer = true");
        }
        if (speedUpProducer) {
          batchSize += 82500;
          System.out.println(" batchSize set to :" + batchSize);
        } else if (speedDownProducer) {
          batchSize -= 82500;
          System.out.println(" batchSize set to :" + batchSize);
        }
        if (batchSize < batchSizeLow) {
          batchSize = batchSizeLow;
          speedDownProducer = false;
        }
        kafkaProducer.send(new ProducerRecord<>(topicName, new byte[batchSize]));
        // System.out.printf("Send %d to %s%n",batchSize, topicName);
        if (batchSize > batchSizeUp) {
          speedUpProducer = false;
          batchSize = batchSizeUp;
        }
        if (!timeAdd && batchSize == batchSizeUp) timeWait -= 10;
        if (timeWait <= 0) {
          long start = System.currentTimeMillis();
          var keepTime = (int) (Math.random() * 200);
          while ((System.currentTimeMillis() - start)/1000 <= keepTime ){
            System.out.println("High Speed Send -" +(System.currentTimeMillis() - start)/1000);
            kafkaProducer.send(new ProducerRecord<>(topicName, new byte[batchSize]));
          }
          timeAdd = true;
        }
        if (timeAdd) timeWait += 10;
        if (timeWait > 1000) {
          timeAdd = false;
          timeWait = 1000;
          speedDownProducer = true;
        }
        TimeUnit.MILLISECONDS.sleep(timeWait);
      }
    }

    @Override
    public String explainArgument() {
      return "(topic name)";
    }
  }

  public static class Consumer implements Workload {
    /**
     * @param bootstrapServer the kafka server addresses
     * @param argument a string argument for this workflow
     * @throws InterruptedException
     */
    @Override
    public void run(String bootstrapServer, String argument) throws InterruptedException {
      final String[] split = argument.split(",");
      var topicName = split[0];
      var groupName = split[1];
      final KafkaConsumer<?, ?> kafkaConsumer1 =
          new KafkaConsumer<>(
              Map.of(
                  ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer",
                  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer",
                  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  bootstrapServer,
                  ConsumerConfig.GROUP_ID_CONFIG,
                  groupName,
                  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                  "earliest"));
      final KafkaConsumer<?, ?> kafkaConsumer2 =
          new KafkaConsumer<>(
              Map.of(
                  ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer",
                  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer",
                  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  bootstrapServer,
                  ConsumerConfig.GROUP_ID_CONFIG,
                  groupName));
      var r = 0;
      kafkaConsumer1.subscribe(Set.of(topicName));
      kafkaConsumer2.subscribe(Set.of(topicName));
      while (true) {
        r = (int) (Math.random() * 200);
        // System.out.println(r);
        if (r == 1) {
          r = 0;
          kafkaConsumer1.seekToBeginning(kafkaConsumer1.assignment());
          System.out.println("assignment =" + kafkaConsumer1.assignment());
          kafkaConsumer1.poll(Duration.ofSeconds(10));
        } else if (r == 2) {
          r = 0;
          for (TopicPartition tp : kafkaConsumer2.assignment())
            kafkaConsumer2.seek(tp, (long) (Math.random() * 1000));
          System.out.println("assignment =" + kafkaConsumer2.assignment());
          kafkaConsumer2.poll(Duration.ofSeconds(10));
        }
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }

    @Override
    public String explainArgument() {
      return "(topic name),(group id)";
    }
  }
  public static void main(String[] args) throws InterruptedException {
    Workload workloadProducer = new BurstLoading.Producer();
    workloadProducer.run("192.168.103.143:10000", "test-1,good");
  }
}
