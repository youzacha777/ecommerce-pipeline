from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

# 취소 이벤트 제너레이터용 플래그 경로
FLAG_PATH = "/app/flags/cancel_active.flag"

def toggle_cancel_generator():
    """취소 이벤트 제너레이터 ON/OFF 토글"""
    os.makedirs(os.path.dirname(FLAG_PATH), exist_ok=True)
    
    if os.path.exists(FLAG_PATH):
        os.remove(FLAG_PATH)
        print("🟥 취소 이벤트 제너레이터 OFF (플래그 파일 삭제 완료)")
    else:
        with open(FLAG_PATH, "w") as f:
            f.write("active")
        print("🟩 취소 이벤트 제너레이터 ON (플래그 파일 생성 완료)")

def check_status():
    """현재 취소 제너레이터 상태 확인"""
    if os.path.exists(FLAG_PATH):
        print("현재 상태: 🟩 ON (취소 이벤트 제너레이터 실행 중)")
    else:
        print("현재 상태: 🟥 OFF (취소 이벤트 제너레이터 대기 중)")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),
    "retries": 0,
}

with DAG(
    dag_id="cancel_event_dag",
    default_args=default_args,
    description="취소 이벤트 제너레이터 ON/OFF 제어용 DAG",
    schedule_interval=None,
    catchup=False,
    tags=["controller", "cancel"],
) as dag:

    status_task = PythonOperator(
        task_id="check_cancel_status",
        python_callable=check_status,
    )

    toggle_task = PythonOperator(
        task_id="toggle_cancel_generator",
        python_callable=toggle_cancel_generator,
    )

    # 실행 순서: 상태 확인 → 토글 실행
    status_task >> toggle_task