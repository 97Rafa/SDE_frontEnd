#!/bin/bash
echo "Waiting for Kafka to be ready..."
sleep 6
kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic data_topic
kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic request_topic
kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic estimation_topic
echo "Kafka topics created successfully!"