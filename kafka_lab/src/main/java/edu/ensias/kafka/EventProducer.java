package edu.ensias.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {
    public static void main(String[] args) throws Exception {
        // Vérifier que le topic est fourni comme argument
        if (args.length == 0) {
            System.out.println("Veuillez entrer le nom du topic en argument.");
            return;
        }

        // Lire le nom du topic fourni comme paramètre
        String topicName = args[0];

        // Accéder aux configurations du producteur
        Properties props = new Properties();

        // Spécifier le serveur Kafka
        props.put("bootstrap.servers", "localhost:9092");

        // Définir un acquittement pour les requêtes du producteur
        props.put("acks", "all");

        // Si la requête échoue, le producteur peut réessayer automatiquement
        props.put("retries", 0);

        // Spécifier la taille du batch (en octets)
        props.put("batch.size", 16384);

        // Contrôler l’espace total de mémoire disponible pour le buffering
        props.put("buffer.memory", 33554432);

        // Sérialiseurs pour la clé et la valeur
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Créer le producteur Kafka
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Envoyer 10 messages
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topicName, Integer.toString(i), "Message " + i));
        }

        System.out.println("Messages envoyés avec succès.");

        // Fermer le producteur
        producer.close();
    }
}
