#!/bin/bash
set -e

# cqlsh 호스트를 Docker Compose 서비스 이름 'cassandra'로 설정
CASSANDRA_HOST="cassandra" 

echo "⏳ Cassandra 준비 대기 중..."

# cqlsh 명령에 호스트 지정
until cqlsh $CASSANDRA_HOST -e "DESCRIBE KEYSPACES;" > /dev/null 2>&1; do
    sleep 3
done

echo "✅ Cassandra 준비 완료. 초기화 시작..."

KEYSPACE="user_events_ks"
# cqlsh 명령에 호스트 지정
if ! cqlsh $CASSANDRA_HOST -e "DESCRIBE KEYSPACE $KEYSPACE;" > /dev/null 2>&1; then
    echo "🚀 Keyspace $KEYSPACE 생성 중..."
    # cqlsh 명령에 호스트 지정
    cqlsh $CASSANDRA_HOST -e "CREATE KEYSPACE $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
else
    echo "ℹ️ Keyspace $KEYSPACE 이미 존재"
fi

# CQL 파일 순차 실행 (경로 수정 및 호스트 지정)
for f in /cassandra-init/*.cql; do
    echo "🚀 실행 중: $f"
    # cqlsh 명령에 호스트 지정
    cqlsh $CASSANDRA_HOST -f "$f"
done

echo "🎉 Cassandra 초기화 완료!"
