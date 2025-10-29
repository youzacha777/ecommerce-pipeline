from prometheus_client import start_http_server, Gauge
import time
import random
import logging
import sys

# -------------------------------
# 1. 로깅 설정 (stdout)
# -------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    force=True
)

# -------------------------------
# 2. 지표 정의
# -------------------------------
cdc_delay = Gauge('cdc_delay_seconds', 'CDC 전송 지연 시간')
slack_response = Gauge('slack_alert_response_seconds', '알림 반응 시간')
data_accuracy = Gauge('data_accuracy_percent', '데이터 적재 정확도')

# -------------------------------
# 3. Prometheus exporter 서버 시작
# -------------------------------
start_http_server(8000)
logging.info("Custom Exporter running on port 8000...")

# -------------------------------
# 4. 지표 업데이트 루프
# -------------------------------
while True:
    cdc_delay.set(random.uniform(1.0, 6.0))
    slack_response.set(random.uniform(1.0, 4.0))
    data_accuracy.set(random.uniform(99.0, 100.0))

    # 로깅으로 값 출력
    logging.info(
        f"cdc_delay={list(cdc_delay.collect())[0].samples[0].value}, "
        f"slack_response={list(slack_response.collect())[0].samples[0].value}, "
        f"data_accuracy={list(data_accuracy.collect())[0].samples[0].value}"
    )

    time.sleep(10)
