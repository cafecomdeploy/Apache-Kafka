package org.example;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class App {

    public static void main(String[] args) {

        String consumerGroup = System.getenv("CONSUMER_GROUP") == null ?
                "group-0" : System.getenv("CONSUMER_GROUP");

        System.out.println("=== Kafka Consumer ===");
        System.out.println("Aguardando mensagens do tópico 'primeiroTopico'. Consumer Group " + consumerGroup);

        String topico = "primeiroTopico";

        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", consumerGroup);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Collections.singletonList(topico));
        try {
            try {
                System.out.println("Aguardando mensagens...");

                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));

                    records.forEach(record -> {
                        System.out.println(
                                "Mensagem: " + record.value() +
                                        " | Partition: " + record.partition() +
                                        " | Offset: " + record.offset()
                        );
                    });
                }

            } catch (Exception ex) {
                System.out.println("Erro ao consumir mensagem: " + ex.getMessage());
            }

        } finally {
            consumer.close();
        }
    }
}