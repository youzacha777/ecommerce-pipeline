#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
cancel_event_generator.py
- Airflow DAG에서 호출하는 cancel 이벤트 생성 모듈
- purchase_table에서 PAID 주문 중 일부를 랜덤으로 선택해 취소 처리
- 주문 수 100건 이상일 때부터 시작
"""

import os
import sys
import random
import time
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import CANCEL_EVENT_ACTIVE_FLAG
from utils.postgres_conn_utils import get_connection, close_connection
from utils.log_utils import setup_logger


logger = setup_logger("cancel_event_generator", "logs/cancel_event_generator.log")


def generate_cancel_events():
    """PAID 상태 주문 중 일부를 랜덤으로 취소 처리"""
    conn = get_connection()
    if conn is None:
        logger.error("DB 연결 실패로 cancel 이벤트 생성 중단")
        return

    try:
        cur = conn.cursor()

        # 전체 주문 수 확인
        cur.execute("SELECT COUNT(*) FROM purchase_table;")
        total_orders = cur.fetchone()[0]

        if total_orders < 50:
            logger.info(f"현재 주문 수: {total_orders}건 → 50건 미만이라 취소 이벤트 생성 건너뜀")
            return

        # 아직 취소되지 않은 주문 조회 (status='PAID')
        cur.execute("SELECT order_id, user_id, product_id FROM purchase_table WHERE status='PAID'")
        purchase_rows = cur.fetchall()

        if not purchase_rows:
            logger.info("취소 가능한 주문이 없습니다.")
            return

        # 전체 중 약 5%를 랜덤 취소 대상으로 선정
        cancel_candidates = random.sample(purchase_rows, k=max(1, len(purchase_rows)//20))

        logger.info(f"{len(cancel_candidates)}건의 주문을 취소 대상으로 선택")

        for order_id, user_id, product_id in cancel_candidates:
            cancel_reason = random.choice(["고객 변심", "상품 품절", "배송 지연", "결제 오류"])
            cancel_time = datetime.now()

            # cancel_table에 삽입 (중복 방지)
            cur.execute("""
                INSERT INTO cancel_table (order_id, user_id, product_id, cancel_reason, cancel_time)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING;
            """, (order_id, user_id, product_id, cancel_reason, cancel_time))

            # 4️⃣ purchase_table 상태 업데이트
            cur.execute("""
                UPDATE purchase_table
                SET status = 'CANCELLED'
                WHERE order_id = %s AND status = 'PAID';
            """, (order_id,))

        logger.info("✅ 취소 이벤트 생성 및 상태 업데이트 완료")

    except Exception as e:
        logger.error(f"취소 이벤트 생성 중 오류 발생: {e}")
    finally:
        close_connection(conn)

def is_active():
    """Airflow DAG에서 만든 플래그 파일 존재 여부 확인"""
    return os.path.exists(CANCEL_EVENT_ACTIVE_FLAG)

def main():
    """Airflow 제어에 따라 ON/OFF 동작하는 메인 루프"""
    # 초기 상태 로그 추가
    if not os.path.exists(CANCEL_EVENT_ACTIVE_FLAG):
        logger.info("초기 상태: 취소 이벤트 제너레이터는 OFF입니다. (Airflow 플래그 파일 없음)")
    else:
        logger.info("초기 상태: 취소 이벤트 제너레이터는 ON입니다.")
    
    logger.info("🚀 Cancel Event Generator 실행 시작")

    while True:
        if not is_active():
            logger.info("Cancel Generator 대기 중... (Airflow OFF 상태)")
            time.sleep(5)
            continue

        generate_cancel_events()
        time.sleep(10)  # 주기적으로 재실행 (10초 간격 등)

if __name__ == "__main__":
    main()



