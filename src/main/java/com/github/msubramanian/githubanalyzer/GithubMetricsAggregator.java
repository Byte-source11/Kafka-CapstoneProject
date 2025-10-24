package com.github.msubramanian.githubanalyzer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class GithubMetricsAggregator {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "github-metrics-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper(); // ✅ Reuse single ObjectMapper

        // Read from the "github-commits" topic
        KStream<String, String> commitsStream = builder.stream("github-commits");

        // Total number of commits (by key)
        KTable<String, Long> totalCommits = commitsStream
                .groupByKey()
                .count(Named.as("total-commits"));

        //  Total number of committers
        KTable<String, Long> totalCommitters = commitsStream
                .groupBy((key, value) -> key)
                .count(Named.as("total-committers"));

        // Top contributors (this will produce per-user commit counts)
        commitsStream
                .groupBy((key, value) -> key)
                .count(Named.as("commits-per-user"))
                .toStream()
                .mapValues((username, count) -> String.format("{\"username\":\"%s\", \"commit_count\":%d}", username, count))
                .to("top-contributors", Produced.with(Serdes.String(), Serdes.String()));

        // Total number of commits for each programming language
        commitsStream
                .mapValues(value -> {
                    try {
                        Map<String, Object> commitData = mapper.readValue(value, Map.class);
                        return (String) commitData.getOrDefault("language", "unknown");
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .groupBy((key, language) -> language, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Named.as("commits-by-language"))
                .toStream()
                .to("commits-by-language", Produced.with(Serdes.String(), Serdes.Long()));

        // Average commits per user (approximation)
        commitsStream
                .groupBy((key, value) -> key)
                .count(Named.as("user-commit-counts"))
                .toStream()
                .mapValues((key, count) -> count.doubleValue()) // convert to double
                .to("average-commits-per-user", Produced.with(Serdes.String(), Serdes.Double()));

        // Total commits per repository
        KTable<String, Long> commitsPerRepo = commitsStream
                .mapValues(value -> {
                    try {
                        Map<String, Object> commitData = mapper.readValue(value, Map.class);
                        return (String) commitData.getOrDefault("repository", "unknown");
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .groupBy((key, repository) -> repository, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Named.as("commits-per-repo"));

        commitsPerRepo
                .toStream()
                .to("total-commits-per-repository", Produced.with(Serdes.String(), Serdes.Long()));

        // Top 3 repositories by commits — cannot use .sorted() or .limit() in Kafka Streams
        // You need a custom Processor or do it externally.
        // For demo, we'll just output all repo counts to a topic.
        commitsPerRepo
                .toStream()
                .to("top-3-repositories-raw", Produced.with(Serdes.String(), Serdes.Long()));

        // Total commits per day
        commitsStream
                .mapValues(value -> {
                    try {
                        Map<String, Object> commitData = mapper.readValue(value, Map.class);
                        String timestamp = (String) commitData.get("timestamp");
                        return timestamp != null ? timestamp.split("T")[0] : "unknown";
                    } catch (Exception e) {
                        return "unknown";
                    }
                })
                .groupBy((key, date) -> date, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Named.as("commits-per-day"))
                .toStream()
                .to("total-commits-per-day", Produced.with(Serdes.String(), Serdes.Long()));

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
