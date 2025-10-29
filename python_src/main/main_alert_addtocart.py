#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
사용자 행동(AddToCart) 알림 프로세서 모듈
Kafka에서 사용자 행동 데이터를 소비하고 설정된 조건에 따라 알림을 생성합니다.
"""

import sys
import os
import json
import time
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    ORDER_TOPIC, ADDTOCART_ALERT_GROUP_ID
)
from utils.kafka_utils import create_consumer, wait_for_topic
# from notifier.slack_notifier import send_slack_message
from utils.log_utils import setup_logger


# 토픽 생성 확인 매개변수 
CHECK_INTERVAL = 10  # 초 단위
BOOTSTRAP_SERVERS = "kafka:29092"

# 로깅 설정
logger = setup_logger("AddToCartAlertProcessor")

def main(topic, group_id):
    """
    Kafka Consumer를 생성하고 AddToCart 이벤트 수신 시 Slack 알림 전송
    """

    # 구독할 토픽 생성 확인
    wait_for_topic(BOOTSTRAP_SERVERS, topic)

    # Kafka Consumer 생성
    consumer = create_consumer(
        topic=topic,
        group_id=group_id,
        auto_offset_reset='latest'
    )

    if consumer is None:
        logger.error("Kafka Consumer 생성 실패. 종료합니다.")
        return

    logger.info(f"AddToCart Alert Processor 시작 (토픽: {topic}, 그룹ID: {group_id})...")

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
                user_id = event.get("user_id")
                product_name = event.get("product_name")
                quantity = event.get("quantity", 1)

                slack_text = (
                    f"사용자 {user_id}가 장바구니에 상품 {product_name} ({quantity}개)를 담았습니다."
                )

                # Slack 알림 대신 로그로 메시지 출력
                logger.info(f"[Slack 알림 시뮬레이션] 사용자 {user_id}가 장바구니에 상품 {product_name} ({quantity}개) 담음")
                # Slack 알림 호출
                # send_slack_message(slack_text)
                logger.info(f"Slack 알림 전송 완료: 사용자 {user_id}, 상품 {product_name}")
            else:
                # AddToCart가 아닌 다른 이벤트 체크
                logger.info(f"처리하지 않는 이벤트 수신 (key: {message.key}, event_type: {event.get('event_type')})")

    except KeyboardInterrupt:
        logger.info("프로세스 종료 중...")
    finally:
        consumer.close()
        logger.info("Kafka Consumer 종료 완료.")


if __name__ == "__main__":
    main(ORDER_TOPIC, ADDTOCART_ALERT_GROUP_ID)

