#!/bin/bash

KAFKA_BIN="/path/to/kafka/bin"
BROKER="localhost:9092"

# Create github-accounts topic
$KAFKA_BIN/kafka-topics.sh --create --topic github-accounts --bootstrap-server $BROKER --replication-factor 2 --partitions 3

# Create github-commits topic
$KAFKA_BIN/kafka-topics.sh --create --topic github-commits --bootstrap-server $BROKER --replication-factor 2 --partitions 3

# Create additional topics for metrics
$KAFKA_BIN/kafka-topics.sh --create --topic top-contributors --bootstrap-server $BROKER --replication-factor 2 --partitions 3
$KAFKA_BIN/kafka-topics.sh --create --topic commits-by-language --bootstrap-server $BROKER --replication-factor 2 --partitions 3