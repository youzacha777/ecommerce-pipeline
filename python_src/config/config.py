#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
설정 파일
Ecommerce Pipeline 프로젝트에서 사용되는 설정값들을 정의
"""

import os
import json
from dotenv import load_dotenv
from pathlib import Path

# Kakfa 설정
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# 토픽 설정
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "user_events")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "alert_events")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "order_events")

# 컨슈머 그룹 ID
ADDTOCART_ALERT_GROUP_ID = os.getenv("ADDTOCART_ALERT_GROUP_ID", "addtocart_alert_group")
ADDTOCART_LOG_GROUP_ID = os.getenv("ADDTOCART_LOG_GROUP_ID", "addtocart_log_group")
PURCHASE_LOG_GROUP_ID = os.getenv("PURCHASE_LOG_GROUP_ID", "purchase_log_group")

# 로깅 설정
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE_MAX_BYTES = int(os.getenv('LOG_FILE_MAX_BYTES', 10485760)) # 10MB
LOG_FILE_BACKUP_COUNT = int(os.getenv('LOG_FILE_BACKUP_COUNT', 5))

# Slack 알림 설정
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', 'your_slack_webhook_url')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', '#stock-data-pipeline')

# PostgreSQL 설정
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "event_db")
DB_USER = os.getenv("DB_USER", "event_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "eventpassword")
ADDCART_TABLE = "cart_table"
PURCHASE_TABLE = "purchase_table"
CANCEL_TABLE = 'cancel_table'

# Postgres 연결용 딕셔너리
POSTGRES_CONFIG = {
    "host": DB_HOST,
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD
}

# Airflow 환경변수
EVENT_ACTIVE_FLAG = os.getenv("EVENT_ACTIVE_FLAG", "/app/flags/event_active.flag")
CANCEL_EVENT_ACTIVE_FLAG = os.getenv("CANCEL_EVENT_ACTIVE_FLAG", "/app/flags/cancel_active.flag")

# 제너레이터 정보

# 유저 설정
CONFIG_DIR = Path(__file__).parent

# users.json 불러오기
with open(CONFIG_DIR / "users.json", "r", encoding="utf-8") as f:
    USERS = json.load(f)

# 상품 정보
PRODUCTS = [
        {"store": "A", "product_id": "P001", "product_name": "초코칩-a", "price": 10300},
        {"store": "A", "product_id": "P002", "product_name": "감자칩-a","price": 15700},
        {"store": "A", "product_id": "P003", "product_name": "롤케익-a","price": 27200},
        {"store": "A", "product_id": "P004", "product_name": "샌드-a","price": 25000},
        {"store": "A", "product_id": "P005", "product_name": "젤리-a","price": 7000},
        {"store": "B", "product_id": "P006", "product_name": "초코칩-b","price": 11300},
        {"store": "B", "product_id": "P007", "product_name": "감자칩-b","price": 13700},
        {"store": "B", "product_id": "P008", "product_name": "롤케익-b","price": 25200},
        {"store": "B", "product_id": "P009", "product_name": "샌드-b","price": 15000},
        {"store": "B", "product_id": "P010", "product_name": "젤리-b","price": 7000},
        {"store": "C", "product_id": "P011", "product_name": "초코칩-c","price": 13300},
        {"store": "C", "product_id": "P012", "product_name": "감자칩-c","price": 12700},
        {"store": "C", "product_id": "P013", "product_name": "롤케익-c","price": 27200},
        {"store": "C", "product_id": "P014", "product_name": "샌드-c","price": 23000},
        {"store": "C", "product_id": "P015", "product_name": "젤리-c","price": 7000},
        {"store": "D", "product_id": "P016", "product_name": "초코칩-d","price": 12200},
        {"store": "D", "product_id": "P017", "product_name": "감자칩-d","price": 5700},
        {"store": "D", "product_id": "P018", "product_name": "롤케익-d","price": 20200},
        {"store": "D", "product_id": "P019", "product_name": "샌드-d","price": 35000},
        {"store": "D", "product_id": "P020", "product_name": "젤리-d","price": 7000}
        ]

KEYWORDS = ["A","B","C","D","초코칩","감자칩","롤케익","샌드","젤리"]

# 신상품 추천 알림용 상품 (Browsing)
NEW_PRODUCT = [
        {"store": "E", "product_id": "P021", "product_name": "입점할인중!-초코칩-e", "price": 10000},
        {"store": "E", "product_id": "P022", "product_name": "입점할인중!-감자칩-e","price": 15000},
        {"store": "E", "product_id": "P023", "product_name": "입점할인중!-롤케익-e","price": 25000},
        {"store": "E", "product_id": "P024", "product_name": "입점할인중!-샌드-e","price": 20000},
        {"store": "E", "product_id": "P025", "product_name": "입점할인중!-젤리-e","price": 6000}
]






