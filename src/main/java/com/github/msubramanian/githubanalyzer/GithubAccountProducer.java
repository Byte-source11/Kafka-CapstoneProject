package com.github.msubramanian.githubanalyzer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GithubAccountProducer {
    public static void main(String[] args) {
        // Kafka producer config
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // Read accounts.json
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, Object>> accounts = mapper.readValue(
                Files.readAllBytes(Paths.get("data/accounts.json")),
                List.class
            );

            for (Map<String, Object> account : accounts) {
                String username = (String) account.get("username");
                String value = mapper.writeValueAsString(account); // send the whole object as JSON
                ProducerRecord<String, String> record = new ProducerRecord<>("github-accounts", username, value);
                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.printf("Sent: %s to partition %d, offset %d\n", value, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
            System.out.println("All accounts sent to Kafka topic 'github-accounts'.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
} 