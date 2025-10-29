#!/bin/bash
# Kafka Connect 초기 Connector 등록 스크립트

# Kafka Connect가 준비될 때까지 반복 확인
while true; do
  status=$(curl -s -o /dev/null -w "%{http_code}" http://kafka-connect:8083/)
  if [ "$status" -eq 200 ]; then
    echo "Kafka Connect is ready!"
    break
  else
    echo "Waiting for Kafka Connect..."
    sleep 2
  fi
done

# alert_events Connector 등록
curl -X POST http://kafka-connect:8083/connectors \
-H "Content-Type: application/json" \
-d @connectors/alert-events-cassandra-sink.json

# order_events Connector 등록
curl -X POST http://kafka-connect:8083/connectors \
-H "Content-Type: application/json" \
-d @connectors/order-events-cassandra-sink.json
