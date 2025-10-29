#!/bin/bash
set -e

# -------------------------------
# 0. 환경 변수 & 경로
# -------------------------------
CONNECT_URL="http://kafka-connect:8083"
CONNECTORS_DIR="/connectors"


# Kafka Connect 워커를 백그라운드에서 시작 (필수 추가)
/etc/confluent/docker/run &

echo "⏳ Waiting for Kafka Connect REST API at $CONNECT_URL..."
until curl -s "$CONNECT_URL/connectors" >/dev/null; do
  echo "REST API not ready, retrying..."
  sleep 5
done
echo "✅ Kafka Connect REST API is ready!"

# -------------------------------
# 1. 커넥터 등록 함수
# -------------------------------
register_connector() {
  local file=$1
  
  # grep과 sed를 사용하여 "name" 필드 값 추출
  local name=$(grep '"name"' "$file" | head -1 | sed -E 's/.*"name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')
  
  if [ -z "$name" ]; then
    echo "❌ Error: Could not extract 'name' from $file. Skipping."
    return 1
  fi

  if ! curl -s -f "$CONNECT_URL/connectors/$name" >/dev/null; then
    echo "➡️ Registering connector: $name"
    # curl -s -X POST ... 로직은 그대로 유지
    curl -s -X POST -H "Content-Type: application/json" --data @"$file" "$CONNECT_URL/connectors"
    echo -e "\n✅ Registered: $name"
  else
    echo "⚠️ Connector already exists: $name"
  fi
}

# -------------------------------
# 2. JSON 파일 순회하며 등록
# -------------------------------
for file in "$CONNECTORS_DIR"/*.json; do
  if [ -f "$file" ]; then
    register_connector "$file"
  else
    echo "⚠️ No connector JSON found at: $file"
  fi
done

echo "🎉 All connectors processed!"

# 3. 😴 백그라운드 워커가 종료되지 않도록 대기
wait