#!/bin/bash

echo "⏳ Waiting for Kafka to start..."
sleep 12

echo "🚀 Creating Topics..."

kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic request_topic --replication-factor 1 --partitions 1 && echo "✅ request_topic created"
kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic data_topic --replication-factor 1 --partitions 1 && echo "✅ data_topic created"
kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic estimation_topic --replication-factor 1 --partitions 1 && echo "✅ estimation_topic created"
kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic logging_topic --replication-factor 1 --partitions 1 && echo "✅ logging_topic created"

echo "🎉 All topics are created."
