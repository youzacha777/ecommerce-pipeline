import json, random, time
from datetime import datetime
import sys
import os
import logging
import uuid

# 상위 디렉토리를 path에 추가해 다른 모듈을 import 할 수 있도록 경로 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC, USERS, PRODUCTS, KEYWORDS, EVENT_ACTIVE_FLAG
from utils.kafka_utils import create_producer

# 로그 설정
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/event_generator.log")
    ]
)
logger = logging.getLogger("event_generator")


def generate_events(producer):
    if not producer:
        logger.error("Kafka Producer가 없습니다. 데이터 생성을 중단합니다.")
        return
    
    # 초기 상태 로그 추가
    if not os.path.exists(EVENT_ACTIVE_FLAG):
        logger.info("초기 상태: 이벤트 제너레이터는 OFF입니다. (Airflow 플래그 파일 없음)")
    else:
        logger.info("초기 상태: 이벤트 제너레이터는 ON입니다.")
    
    logger.info(f"🚀 데이터 생성 시작")

    users = USERS 
    products = PRODUCTS
    keywords = KEYWORDS

    try:
        while True:
            # On / Off 제어
            if not is_active():
                logger.info("Event Generator 대기 중... (Airflow OFF 상태)")
                time.sleep(3)
                continue
            try:
                # 이벤트 발생 및 이벤트별 메시지 작성
                event_type = random.choices(
                    ['Browsing','Search','NextPageClick','AddToCart','Purchase'], weights=[0.1,0.2,0.2,0.2,0.3])[0]
                
                user = random.choice(users)
                
                event = {
                    "user_id": user['user_id'],
                    "user_name": user['name'],
                    "email": user['email'],
                    "event_type": event_type,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }

                if event_type == 'Purchase':
                    # 구매 확정
                    product = random.choice(products)
                    product_id = product['product_id']
                    product_name = product['product_name']
                    quantity = random.randint(1,3)
                    price = product["price"]
                    event.update({
                        "order_id": f"ecpl-{product['store']}-{uuid.uuid4().hex[:8]}",
                        "product_id": product_id,
                        "product_name": product_name,
                        "quantity": quantity,
                        "price" :  price,
                        "total" : quantity * price,
                        "status" : "PAID"
                    })
                
                elif event_type == 'AddToCart':
                    # 검색한 상품을 장바구니 버튼을 눌러서 담음
                    product = random.choice(products)
                    product_id = product['product_id']
                    product_name = product['product_name']
                    quantity = random.randint(1,3)
                    price = product["price"]
                    event.update({
                        "order_id": f"ecpl-{product['store']}-{uuid.uuid4().hex[:8]}",
                        "product_id": product_id,
                        "product_name": product_name,
                        "quantity": quantity,
                        "price" :  price,
                        "total" : quantity * price,
                        "status" : "UNPAID"
                    })

                elif event_type == 'NextPageClick':
                    # 키워드 검색 후 조회 뒤 다음 페이지 버튼 누름
                    current_page = random.randint(1,10)
                    next_page = current_page + 1
                    keyword = random.choice(keywords)
                    store = {'A','B','C','D'}
                    if keyword in store:
                        displayed_products = [p['product_id'] for p in products if p['store'] == keyword]
                    else:
                        displayed_products = [p['product_id'] for p in products if keyword in p['product_name']]

                    event.update({
                        "event_id": str(uuid.uuid4()),
                        "keyword": keyword,
                        "current_page" : current_page,
                        "next_page" : next_page,
                        "products_displayed" : displayed_products
                    })

                elif event_type == 'Search':
                    # 키워드 검색 후 조회
                    keyword = random.choice(keywords)
                    store = {'A','B','C','D','E'}
                    if keyword in store:
                        displayed_products = [p['product_id'] for p in products if p['store'] == keyword]
                    else:
                        displayed_products = [p['product_id'] for p in products if keyword in p['product_name']]

                    event.update({
                        "event_id": str(uuid.uuid4()),
                        "keyword": keyword,
                        "products_displayed" : displayed_products,
                        "results_count": len(displayed_products)
                    })

                elif event_type == 'Browsing':
                    # 단순 페이지 조회, 아무 상품 선택 안 함
                    event.update({
                        "event_id": str(uuid.uuid4()),
                        "note": "User is just browsing",
                    })

                # 카프카로 전송
                producer.send(
                    INPUT_TOPIC,
                    key=event["user_id"],  
                    value=event
                )
                logger.info(f"Sending key: {event['user_id']}")
                logger.info(f"Kafka Producer로 메시지 전송 완료(Event sent): {event}")
                time.sleep(random.uniform(0.1,1))

            except Exception as e:
                logger.error(f"이벤트 생성 및 메시지 전송 중 오류 발생 : {e}")
                logger.info("잠시 후 재시도합니다.")
                time.sleep(10)
        
    except KeyboardInterrupt:
        producer.close()
        logger.info("사용자 요청에 의해 이벤트 생성을 종료합니다.")

def is_active():
    """Airflow에서 켰는지 확인"""
    return os.path.exists(EVENT_ACTIVE_FLAG)


def main():
    producer = create_producer() # Kafka 유틸 활용
    generate_events(producer)


if __name__ == "__main__":
    main()
                