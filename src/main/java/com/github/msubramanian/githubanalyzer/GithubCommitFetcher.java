package com.github.msubramanian.githubanalyzer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import java.time.Duration;
import java.util.*;

public class GithubCommitFetcher {
    private static final String GITHUB_API_URL = "https://api.github.com";
    private static final String GITHUB_TOKEN = System.getenv("GITHUB_TOKEN"); // Optional: set your GitHub token as env var for higher rate limits

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "github-commit-fetcher-group-NEW");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092,localhost:9093");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("acks", "all");

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             Producer<String, String> producer = new KafkaProducer<>(producerProps);
             CloseableHttpClient httpClient = HttpClients.createDefault()) {

            consumer.subscribe(Collections.singletonList("github-accounts"));
            System.out.println("Listening for accounts on 'github-accounts' topic...");
            int count = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                System.out.println("Polled " + records.count() + " records");
               if(records.count() == 0) {
                count++;
                if(count >= 3) {
                    System.out.println("No records found for 3 seconds, exiting");
                    // System.exit(0);
                    break;
                }
               }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Got record: " + record.value());
                    Map<String, Object> account = mapper.readValue(record.value(), new TypeReference<Map<String, Object>>(){});
                    String username = (String) account.get("username");
                    String interval = (String) account.get("interval");
                    System.out.printf("Processing user: %s, interval: %s\n", username, interval);

                    // Calculate since date
                    String since = getSinceDate(interval);
                    String url = GITHUB_API_URL + "/users/" + username + "/events";
                    HttpGet request = new HttpGet(url);
                    if (GITHUB_TOKEN != null && !GITHUB_TOKEN.isEmpty()) {
                        request.addHeader("Authorization", "token " + GITHUB_TOKEN);
                    }
                    ClassicHttpResponse response = (ClassicHttpResponse) httpClient.execute(request);
                    String json = EntityUtils.toString(response.getEntity());
                    List<Map<String, Object>> events = mapper.readValue(json, new TypeReference<List<Map<String, Object>>>(){});

                    for (Map<String, Object> event : events) {
                        if ("PushEvent".equals(event.get("type"))) {
                            Map<String, Object> repo = (Map<String, Object>) event.get("repo");
                            String repoName = repo != null ? (String) repo.get("name") : "unknown";
                            String createdAt = (String) event.get("created_at");
                            if (createdAt.compareTo(since) >= 0) {
                                Map<String, Object> commitMsg = new HashMap<>();
                                commitMsg.put("username", username);
                                commitMsg.put("repo", repoName);
                                commitMsg.put("created_at", createdAt);
                                commitMsg.put("raw_event", event);
                                String commitJson = mapper.writeValueAsString(commitMsg);
                                ProducerRecord<String, String> commitRecord = new ProducerRecord<>("github-commits", username, commitJson);
                                producer.send(commitRecord);
                                System.out.printf("Produced commit for %s in repo %s at %s\n", username, repoName, createdAt);
                            }
                        }
                    }
                    if(records.count() == 0) {
                        System.out.println("No records found");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper to get ISO date string for 'since' parameter
    private static String getSinceDate(String interval) {
        // Only supports 'd' (days) and 'w' (weeks) for now
        int amount = Integer.parseInt(interval.substring(0, interval.length() - 1));
        char unit = interval.charAt(interval.length() - 1);
        java.time.ZonedDateTime now = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC);
        java.time.ZonedDateTime since;
        if (unit == 'd') {
            since = now.minusDays(amount);
        } else if (unit == 'w') {
            since = now.minusWeeks(amount);
        } else {
            since = now.minusDays(1); // default to 1 day
        }
        return since.toString();
    }
} 