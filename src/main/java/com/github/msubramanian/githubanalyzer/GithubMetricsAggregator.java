package com.github.msubramanian.githubanalyzer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.stream.Collectors;

public class GithubMetricsAggregator {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "github-metrics-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Read from the "github-commits" topic
        KStream<String, String> commitsStream = builder.stream("github-commits");

        // Total number of commits
        KTable<String, Long> totalCommits = commitsStream
                .groupByKey()
                .count();

        // Total number of committers
        KTable<String, Long> totalCommitters = commitsStream
                .groupBy((key, value) -> key) // Group by username
                .count();

        // Top 5 contributors by number of commits
        commitsStream
                .groupBy((key, value) -> key)
                .count()
                .toStream()
                .mapValues((key, value) -> Map.of("username", key, "commit_count", value))
                .to("top-contributors");

        // Total number of commits for each programming language
        commitsStream
                .mapValues(value -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> commitData = mapper.readValue(value, Map.class);
                        return (String) commitData.get("language");
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .groupBy((key, language) -> language)
                .count()
                .toStream()
                .to("commits-by-language");

        // Average commits per user
        commitsStream
                .groupBy((key, value) -> key)
                .count()
                .toStream()
                .mapValues((key, value) -> value / 1.0) // Convert to double for average
                .to("average-commits-per-user");

        // Total commits per repository
        commitsStream
                .mapValues(value -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> commitData = mapper.readValue(value, Map.class);
                        return (String) commitData.get("repository");
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .groupBy((key, repository) -> repository)
                .count()
                .toStream()
                .to("total-commits-per-repository");

        // Top 3 repositories by number of commits
        commitsStream
                .mapValues(value -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> commitData = mapper.readValue(value, Map.class);
                        return (String) commitData.get("repository");
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .groupBy((key, repository) -> repository)
                .count()
                .toStream()
                .sorted((entry1, entry2) -> Long.compare(entry2.getValue(), entry1.getValue())) // Sort by count descending
                .limit(3) // Limit to top 3
                .to("top-3-repositories");

        // Total commits per day
        commitsStream
                .mapValues(value -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> commitData = mapper.readValue(value, Map.class);
                        String timestamp = (String) commitData.get("timestamp");
                        return timestamp.split("T")[0]; // Extract date part
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .groupBy((key, date) -> date)
                .count()
                .toStream()
                .to("total-commits-per-day");

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}