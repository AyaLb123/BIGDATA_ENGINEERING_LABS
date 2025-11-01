package edu.ensias.kafka;

import java.util.Properties;
import java.util.Arrays;
import java.time.Duration;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EventConsumer {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Veuillez entrer le nom du topic en argument.");
            return;
        }

        String topicName = args[0];

        // Configuration du consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = null;

        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topicName));

            System.out.println("Souscrit au topic : " + topicName);

            while (true) {
                // Poll avec un timeout pour récupérer les messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("Erreur lors de la consommation : " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close(); // Fermeture du consumer proprement
                System.out.println("Consumer fermé.");
            }
        }
    }
}
