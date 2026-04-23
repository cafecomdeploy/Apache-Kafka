package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaProducerException;

import java.util.Properties;
import java.util.Scanner;

public class App
{
    public static void main( String[] args )
    {
        String topico = "primeiroTopico";

        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config);
             Scanner scanner = new Scanner(System.in)) {

            while (true) {
                System.out.print("> ");
                String input = scanner.nextLine();

                if (input == null || input.trim().isEmpty()) {
                    continue;
                }

                if (input.equalsIgnoreCase("sair")) {
                    System.out.println("Encerrando o producer...");
                    break;
                }

                try {
                    producer.send(new ProducerRecord<>(topico, input), (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println(
                                    "Mensagem enviada para o tópico '" + metadata.topic() + "'" +
                                            " [Partition: " + metadata.partition() +
                                            ", Offset: " + metadata.offset() + "]"
                            );
                        } else {
                            System.out.println("Erro ao enviar mensagem: " + exception.getMessage());
                        }
                    });
                } catch (KafkaProducerException e) {
                    System.out.println("Erro inesperado: " + e.getMessage());
                }
            }
        }
    }
}
