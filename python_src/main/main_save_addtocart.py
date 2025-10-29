#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AddToCart DB 적재 프로세서
Kafka에서 구매 이벤트를 소비하고 Postgres에 저장합니다.
"""

import sys
import os
import json
import time
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.kafka_utils import create_consumer, wait_for_topic
from utils.postgres_conn_utils import get_connection
from processors.addtocart_db_processor import insert_addtocart_event
from utils.log_utils import setup_logger

from config.config import (
    ORDER_TOPIC, ADDTOCART_LOG_GROUP_ID
)

# 토픽 생성 확인 매개변수 
CHECK_INTERVAL = 10  # 초 단위
BOOTSTRAP_SERVERS = "kafka:29092"

logger = setup_logger("addtocart_db_processor", "logs/addtocart_db_processor.log")


def main(topic, group_id):

    # 구독할 토픽 생성 확인
    wait_for_topic(BOOTSTRAP_SERVERS, topic)

    # 컨슈머 생성
    consumer = create_consumer(
        topic=topic,
        group_id=group_id,
        auto_offset_reset='latest'
    )

    if consumer is None:
        logger.error("Kafka Consumer 생성 실패. 종료합니다.")
        return

    # Postgres 연결
    conn = get_connection()
    if conn is None:
        logger.error("Postgres 연결 실패. 종료합니다.")
        return

    logger.info(f"AddToCart DB Processor 시작 (토픽: {topic}, 그룹ID: {group_id})...")

    try:
        for message in consumer:
            event = message.value  # 이미 safe_deserialize가 적용됨
            
            # 메시지 타입 검증
            # 툼스톤 메시지인 경우
            if event is None:
                logger.info(f"Tombstone 메시지 수신 (키: {message.key})")
                continue  # tombstone 메시지 무시
            # 메시지가 잘못된 경우
            if not isinstance(event, dict):
                logger.info(f"잘못된 메시지 포맷: {event}")
                continue
            
            # AddToCart 이벤트만 처리
            if event.get("event_type") == "AddToCart":
                insert_addtocart_event(conn, event)
                logger.info(f"DB 적재 완료: 사용자 {event.get('user_id')}, 상품 {event.get('product_name')}")
            else:
                # AddToCart가 아닌 다른 이벤트 체크
                logger.info(f"처리하지 않는 이벤트 수신 (key: {message.key}, event_type: {event.get('event_type')})")

    except KeyboardInterrupt:
        logger.info("프로세스 종료 중...")
    finally:
        consumer.close()
        conn.close()
        logger.info("Kafka Consumer 및 Postgres 연결 종료 완료.")


if __name__ == "__main__":
    main(ORDER_TOPIC, ADDTOCART_LOG_GROUP_ID)
