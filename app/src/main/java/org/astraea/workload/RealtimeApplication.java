package org.astraea.workload;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RealtimeApplication {
  public static class Producer implements Workload {
    /** @param argument topicName */
    @Override
    public void run(String bootstrapServer, String argument) {
      final String[] split = argument.split(",");
      var topicName = split[0];
      final KafkaProducer<byte[], byte[]> kafkaProducer =
          org.astraea.producer.Producer.of(bootstrapServer).kafkaProducer();
      while (true) kafkaProducer.send(new ProducerRecord<>(topicName, new byte[10]));
    }

    @Override
    public String explainArgument() {
      return "(topic name)";
    }
  }

  public static class Consumer implements Workload {
    /**
     * @param bootstrapServer
     * @param argument topicName
     */
    @Override
    public void run(String bootstrapServer, String argument) {
      final String[] split = argument.split(",");
      var topicName = split[0];
      var groupName = split[1];
      final KafkaConsumer<?, ?> kafkaConsumer =
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
      kafkaConsumer.subscribe(Set.of(topicName));
      while (true) {
        kafkaConsumer.poll(Duration.ofSeconds(1L));
      }
    }

    @Override
    public String explainArgument() {
      return "(topic name),(group id)";
    }
  }

  public static void main(String[] args) throws InterruptedException {
    Workload workloadProducer = new RealtimeApplication.Consumer();
    workloadProducer.run("192.168.103.39:11300", "test-1,good");
  }
}
