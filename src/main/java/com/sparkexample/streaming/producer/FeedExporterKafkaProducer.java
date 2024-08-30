package com.sparkexample.streaming.producer;

import ai.autonomic.feed.FeedMetricExporterOutputMessage;
import ai.autonomic.feed.FlowMetric;
import com.google.protobuf.Timestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FeedExporterKafkaProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Configure Kafka Producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9094"); // Kafka server address
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());

        // Create Kafka Producer
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        // Produce multiple messages
        for (int i = 0; i < 2; i++) {
            // Create a protobuf message
            FeedMetricExporterOutputMessage message = FeedMetricExporterOutputMessage.newBuilder()
                    .setTmcArrivalTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build())
                    .setVin("1HGCM82633A123456" + i)
                    .addDataUris("http://example.com/data1")
                    .setDeviceType("sensor")
                    .setRegion("US")
                    .setProtocolVersion("1.0")
                    .setFlowMetric(FlowMetric.newBuilder()
                            .setFlowAui("flow_123")
                            .setFlowName("Example Flow")
                            .setFlowTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build())
                            .build())
                    .addCorrelationIds("correlation_id_1")
                    .setAssetId("asset_123")
                    .addExpirationLengthTypes("type1")
                    .build();

            // Create a ProducerRecord
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("feed-exported-metrics", "key"+i, message.toByteArray());

            // Send the record
           RecordMetadata recordMetadata = producer.send(record).get();

            System.out.println("Produced record: " + recordMetadata);
        }


        // Close the producer
        producer.close();
    }
}