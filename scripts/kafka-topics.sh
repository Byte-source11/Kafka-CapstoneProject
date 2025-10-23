#!/bin/bash

# Try to locate Kafka binaries automatically
if command -v kafka-topics.sh &> /dev/null; then
    KAFKA_BIN=""
elif [ -d "/usr/local/kafka/bin" ]; then
    KAFKA_BIN="/usr/local/kafka/bin/"
elif [ -d "/opt/kafka/bin" ]; then
    KAFKA_BIN="/opt/kafka/bin/"
else
    echo "Error: Kafka binaries not found. Please set the KAFKA_BIN environment variable."
    exit 1
fi

BROKER="localhost:9092"

# Create github-accounts topic
${KAFKA_BIN}kafka-topics.sh --create --topic github-accounts --bootstrap-server $BROKER --replication-factor 2 --partitions 3

# Create github-commits topic
${KAFKA_BIN}kafka-topics.sh --create --topic github-commits --bootstrap-server $BROKER --replication-factor 2 --partitions 3

# Create additional topics for metrics
${KAFKA_BIN}kafka-topics.sh --create --topic top-contributors --bootstrap-server $BROKER --replication-factor 2 --partitions 3
${KAFKA_BIN}kafka-topics.sh --create --topic commits-by-language --bootstrap-server $BROKER --replication-factor 2 --partitions 3