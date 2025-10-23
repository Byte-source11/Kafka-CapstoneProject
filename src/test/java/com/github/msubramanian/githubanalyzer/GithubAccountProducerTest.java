package com.github.msubramanian.githubanalyzer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.Map;

@EmbeddedKafka(partitions = 1, topics = {"github-accounts"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class GithubAccountProducerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void testProducerInitialization() {
        GithubAccountProducer producer = new GithubAccountProducer();
        assertNotNull(producer, "Producer should be initialized");
    }

    @Test
    public void testProducerSendsMessage() {
        // Producer configuration
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        var producer = producerFactory.createProducer();

        // Send a message to the github-accounts topic
        producer.send(new ProducerRecord<>("github-accounts", "test-key", "test-value"));
        producer.flush();

        // Consumer configuration
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        var consumer = consumerFactory.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "github-accounts");

        // Validate the message
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "github-accounts");
        assertEquals("test-key", record.key());
        assertEquals("test-value", record.value());
    }
}