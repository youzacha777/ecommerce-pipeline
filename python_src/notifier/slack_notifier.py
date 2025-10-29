#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Slack 알림 모듈
Slack 웹훅을 사용하여 각 사용자 행동별 알림을 전송합니다.
"""

import json
import requests
import logging
import sys
from datetime import datetime
import os

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.log_utils import setup_logger
from config.config import SLACK_WEBHOOK_URL, SLACK_CHANNEL

# 로깅 설정
logger = setup_logger("slack_notifier", "logs/slack_notifier.log")

class SlackNotifier:
    """Slack 알림 클래스"""

    def __init__(self, webhook_url=None, channel=None):
        """초기화 함수"""
        self.webhook_url = webhook_url or SLACK_WEBHOOK_URL
        self.channel = channel or SLACK_CHANNEL

        if not self.webhook_url:
            logger.warning("Slack Webhook URL이 설정되지 않았습니다. Slack 알림이 비활성화 됩니다.")
        else:
            logger.info(f"Slack 알림 초기화 완료. 채널: {self.channel}")

    def send_addtocart_alert(self, product_name: str, quantity: int):
        """장바구니 알림 메시지 전송"""
        if not self.webhook_url:
            logger.warning("Slack Webhook URL이 없어 메시지를 전송할 수 없습니다.")
            return False

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "장바구니 알림 🛒"}
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"👉 고객님이 담아두신 *{product_name}* {quantity}개가 아직 결제되지 않았습니다."
                }
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": "⏰ 1시간 내 구매하지 않으면 품절될 수 있어요!"}
                ]
            }
        ]

        payload = {
            "text": "장바구니 알림",
            "blocks": blocks,
            "channel": self.channel
        }

        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"}
            )

            if response.status_code == 200:
                logger.info(f"Slack 메시지 전송 성공: {product_name}, {quantity}")
                return True
            else:
                logger.error(f"Slack 메시지 전송 실패: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Slack 메시지 전송 중 오류 발생: {e}")
            return False
