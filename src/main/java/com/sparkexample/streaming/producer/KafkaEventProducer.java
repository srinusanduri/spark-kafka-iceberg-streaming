package com.sparkexample.streaming.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;
import java.util.UUID;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class KafkaEventProducer {

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();

        String bootstrapServers = "localhost:9094";
        String topic = "sample-stream-topic";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 1; i++) { // Send 10 messages
                try {
                    ObjectNode eventJson = objectMapper.createObjectNode();
                    eventJson.put("id", UUID.randomUUID().toString());
                    eventJson.put("timestamp", Instant.now().toString());
                    eventJson.put("value", new Random().nextDouble() * 100);

                    String eventString = objectMapper.writeValueAsString(eventJson);

                    System.out.println("eventString: " + eventString);

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventString);

                    // Send data and wait for acknowledgment
                    RecordMetadata metadata = producer.send(record).get();

                    System.out.println("Sent: " + eventString + " to partition " + metadata.partition() + " with offset " + metadata.offset());

                    Thread.sleep(1000); // Wait for 1 second before sending the next message
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("Error sending message: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            System.out.println("All messages sent successfully.");
        } catch (Exception e) {
            System.err.println("Error with producer: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Producer finished and closed.");
    }
}