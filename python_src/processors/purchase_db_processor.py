#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import sys
import os
import psycopg2
from datetime import datetime
from utils.log_utils import setup_logger

logger = setup_logger("purchase_db_processor", "logs/purchase_db_processor.log")

def insert_purchase_event(conn, event):
    """구매 이벤트를 Postgres에 적재"""
    if not event:
        logger.warning("빈 이벤트 수신, DB 적재 건너뜀")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO purchase_table (
                    order_id, user_id, user_name, email,
                    product_id, product_name, quantity, price,
                    total, status, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                event.get("order_id"),
                event.get("user_id"),
                event.get("user_name"),
                event.get("email"),
                event.get("product_id"),
                event.get("product_name"),
                event.get("quantity"),
                event.get("price"),
                event.get("total"),
                event.get("status"),
                datetime.now()
            ))
            conn.commit()
            logger.info(f"Purchase 이벤트 적재 완료: {event.get('order_id')}")
    except Exception as e:
        conn.rollback()
        logger.error(f"DB 적재 실패: {e}")