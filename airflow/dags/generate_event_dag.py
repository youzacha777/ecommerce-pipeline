from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

FLAG_PATH = "/app/flags/event_active.flag"

def toggle_event_generator():
    """이벤트 제너레이터 ON/OFF 토글"""
    os.makedirs(os.path.dirname(FLAG_PATH), exist_ok=True)
    
    if os.path.exists(FLAG_PATH):
        os.remove(FLAG_PATH)
        print("🟥 이벤트 제너레이터 OFF (플래그 파일 삭제 완료)")
    else:
        with open(FLAG_PATH, "w") as f:
            f.write("active")
        print("🟩 이벤트 제너레이터 ON (플래그 파일 생성 완료)")

def check_status():
    """현재 상태 확인"""
    if os.path.exists(FLAG_PATH):
        print("현재 상태: 🟩 ON (이벤트 제너레이터 실행 중)")
    else:
        print("현재 상태: 🟥 OFF (이벤트 제너레이터 대기 중)")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),
    "retries": 0,
}

with DAG(
    "generate_event_dag",
    default_args=default_args,
    description="이벤트 제너레이터 ON/OFF 토글 DAG",
    schedule_interval=None,
    catchup=False,
    tags=["controller", "event"],
) as dag:

    toggle_task = PythonOperator(
        task_id="toggle_event_generator",
        python_callable=toggle_event_generator,
    )

    status_task = PythonOperator(
        task_id="check_event_status",
        python_callable=check_status,
    )

    status_task >> toggle_task
