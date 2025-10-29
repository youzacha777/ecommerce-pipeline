#!/bin/bash
set -e

# Kafka가 준비될 때까지 대기
echo "Waiting for Kafka broker to be ready..."
until kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "Kafka not ready yet, sleeping 3s..."
  sleep 3
done

echo "Kafka is ready! Creating topics..."

# 토픽 생성
kafka-topics --create --bootstrap-server kafka:29092 --replication-factor 1 --partitions 1 --topic user_events
kafka-topics --create --bootstrap-server kafka:29092 --replication-factor 1 --partitions 1 --topic alert_events
kafka-topics --create --bootstrap-server kafka:29092 --replication-factor 1 --partitions 1 --topic order_events

echo "Topics created successfully."
