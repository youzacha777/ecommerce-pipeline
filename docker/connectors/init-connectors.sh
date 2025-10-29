#!/bin/bash
set -e

# -------------------------------
# 환경 변수
# -------------------------------
CONNECT_URL="http://localhost:8083"
BOOTSTRAP_SERVER="kafka:29092"

# Kafka Connect에 등록할 JSON 커넥터 목록
CONNECTOR_FILES=(
  "/connectors/postgres-debezium-source.json"
  "/connectors/alert-events-cassandra-sink.json"
  "/connectors/order-events-cassandra-sink.json"
  "/connectors/bigquery-sink.json"
)

# Debezium이 생성할 CDC 토픽 목록 (대기용)
TOPICS=("cdc.cart_table" "cdc.purchase_table" "cdc.cancel_table")

# -------------------------------
# 1. Kafka Connect 준비 대기
# -------------------------------
echo "⏳ Waiting for Kafka Connect REST API at $CONNECT_URL..."
until curl -s "$CONNECT_URL/" >/dev/null; do
  echo "Kafka Connect not ready yet, retrying in 5s..."
  sleep 5
done
echo "✅ Kafka Connect REST API is ready!"


# -------------------------------
# 2. 커넥터 등록 함수
# -------------------------------
register_connector() {
  local json_file=$1
  local connector_name=$(grep -oP '"name"\s*:\s*"\K[^"]+' "$json_file" | head -n 1)
  local connector_url="$CONNECT_URL/connectors/$connector_name"

  local http_code=$(curl -o /dev/null -s -w "%{http_code}" "$connector_url")

  if [ "$http_code" = "404" ]; then
    echo "➡️ Registering connector: $connector_name"
    curl -s -X POST -H "Content-Type: application/json" --data @"$json_file" "$CONNECT_URL/connectors"
    echo "✅ $connector_name registered"

  elif [ "$http_code" = "200" ]; then
    echo "⚠️ $connector_name already exists, skipping"

  else
    echo "❌ Error checking connector $connector_name. HTTP code: $http_code. Skipping registration."
  fi
}

# -------------------------------
# 3. 모든 커넥터 등록
# -------------------------------
for file in "${CONNECTOR_FILES[@]}"; do
  if [ -f "$file" ]; then
    register_connector "$file"
  else
    echo "⚠️ Connector JSON not found: $file"
  fi
done

echo "🎉 All connectors processed successfully!"
