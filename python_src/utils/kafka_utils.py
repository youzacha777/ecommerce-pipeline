#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka 유틸리티 모듈
Producer와 Consumer 생성
"""

import json
import logging
import sys
import os
import time
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import 할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BOOTSTRAP_SERVERS

# 로깅 설정
logger = logging.getLogger("kafka_utils")


def create_producer():
    """Kafka Producer 생성"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info(f"Kafka Producer 연결 성공: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Kafka Producer 연결 실패: {e}")
        return None

def create_consumer(topic, group_id=None, auto_offset_reset='latest', enable_auto_commit=True):
    """Kafka Consumer 생성"""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            value_deserializer=safe_deserialize
        )
        logger.info(f"Kafka Consumer 연결 성공: {KAFKA_BOOTSTRAP_SERVERS}, 토픽: {topic}")
        return consumer
    except Exception as e:
        logger.error(f"Kafka Consumer 연결 실패: {e}")
        return None

    
def wait_for_topic(bootstrap_servers, topic_name, timeout=300, interval=10):
    """
    Kafka 토픽이 생성될 때까지 대기
    """
    start_time = time.time()
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            topics = admin_client.list_topics()
        except KafkaError as e:
            logger.warning(f"Kafka 연결 실패: {e}, {interval}초 후 재시도")
            time.sleep(interval)
            continue

        if topic_name in topics:
            logger.info(f"토픽 '{topic_name}' 생성됨")
            return True
        elif time.time() - start_time > timeout:
            logger.error(f"토픽 '{topic_name}'가 {timeout}초 내 생성되지 않음")
            return False
        else:
            logger.info(f"토픽 '{topic_name}' 미생성, {interval}초 후 재시도")
            time.sleep(interval)

def safe_deserialize(value):
    if not value:  # 메시지가 None 또는 b''이면
        logger.info(f"빈 메시지 감지: {value}")
        return None
    try:
        return json.loads(value.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.info(f"JSON 디코딩 실패: {value}, 오류: {e}")
        return None

