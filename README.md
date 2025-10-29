# 실시간 E-Commerce 이벤트 데이터 파이프라인 구축 및 모니터링

## 1. 프로젝트 개요

- **프로젝트명**: 실시간 E-Commerce 이벤트 데이터 파이프라인 구축 및 모니터링
- **도입 배경**:  
  실제 이커머스 서비스 환경을 가정하여 사용자 이벤트를 실시간으로 처리하고, 주문 데이터를 CDC를 통해 데이터 웨어하우스로 전송하여 전체 파이프라인을 모니터링할 수 있는 시스템 구축.

---

## 2. 목표 및 KPI

- **목표**
  - 데이터 흐름 자동화: 사용자 이벤트 및 주문 데이터를 실시간으로 수집·처리되는 end-to-end 파이프라인 구축
  - 운영 안정성 확보: Kafka 및 DB 상태를 실시간으로 모니터링하여 장애나 지연을 즉시 감지
  - 데이터 분석 기반 마련: CDC를 통해 실시간 주문 데이터를 BigQuery로 적재, 분석 환경 기반 구축
  - 알림 자동화 구현: 특정 사용자 이벤트(Browsing, Search, NextPageClick 등)에 대한 맞춤형 마케팅 알림 처리

- **KPI**
    
| 지표명 | 측정 기준 | 목표 |
| --- | --- | --- |
| 이벤트 처리 지연 시간 | Kafka 프로듀싱 → 소비까지 걸린 평균 시간 | 2초 이내 |
| 메시지 처리 속도 | 메시지 발생 속도(0.1s~1s) 가정 초당 처리 수 | 초당 2건 이상 |
| CDC 전송 지연 | Postgres → BigQuery 반영까지 걸린 시간 | 5초 이내 |
| 알림 반응 시간 | 이벤트 발생 → Slack 알림 수신까지 시간 | 3초 이내 |
| 데이터 적재 정확도 | CDC 반영 후 BigQuery의 레코드 일치율 | 99.5% 이상 |

---

## 3. 아키텍처

### 작업 흐름

1. **사용자 행동 이벤트 데이터 생성 (Event Generation & Ingestion Layer)**
   - 구성 요소: Event Generator, Cancel Generator, Kafka Producer, Kafka Topic (`user_events`)
   - 역할: 사용자 행동 이벤트 생성, 구매 이벤트 취소 데이터 생성
   - 이벤트 종류: Browsing, Search, NextPageClick, AddToCart, Purchase
   - 특징:
     - JSON Schema 형태로 Kafka 전송
     - Key 기반 파티셔닝으로 순서 보장
     - Kafka Streams로 alert_events / order_events 분기
     
2. **실시간 이벤트 처리 및 분기 (Streaming Processing Layer)**
   - 구성 요소: Kafka Streams, Kafka Connect, Kafka Topic(alert_events, order_events)
   - 역할:
     - `user_events` 토픽을 분기
     - 분기된 토픽 Cassandra/Postgres에 적재
     - Slack API 연동 실시간 알림 발송
   - 특징:
     - Kafka Streams 기반 분기
     - Kafka Connect 통한 DB 연동
     - 이벤트 기반 아키텍처 장점 확보

3. **데이터 저장 및 CDC (Storage & Change Data Capture Layer)**
   - 구성 요소: PostgreSQL, Cassandra, Debezium Connector, Kafka Connect
   - 역할:
     - Postgres: 트랜잭션성 데이터 저장
     - Cassandra: 로그성 데이터 저장
     - Debezium: Postgres 변경 사항 실시간 감지
   - 특징:
     - RDB + NoSQL 병행
     - DB → Kafka → BigQuery 실시간 연계
     - 확장성 및 장애 격리, 추적 및 감사 용이

4. **데이터 웨어하우스 및 분석 (Data Warehouse Layer)**
   - 구성 요소: BigQuery
   - 역할: CDC 실시간 적재, 분석용 데이터 마트
   - 특징:
     - 테이블 파티셔닝 적용 (하루 단위)
     - 실시간 데이터 반영 및 분석 활용

5. **모니터링 및 알림 (Monitoring & Alerting Layer)**
   - 구성 요소: Prometheus, cAdvisor, Grafana, Slack
   - 역할: Kafka 처리율, CDC 지연, 컨테이너 리소스, 알림 상태 모니터링
   - 특징: 실시간 운영 가시성 확보, 운영 안정성 강화, 확장성/유연성 제공

- **구조도**  
![시스템 아키텍쳐](/images/architecture.jpeg)

---

## 4. 구현 내용

1. **Docker 기반 환경 구성**
   - 컨테이너: Zookeeper, Kafka, Kafka Connect, PostgreSQL, Cassandra, java-streams, event-generator, cancel-generator, Airflow, Prometheus, cAdvisor, Grafana
   - 핵심: docker-compose로 서비스 컨테이너화, init 컨테이너로 초기화 자동화, depends_on과 네트워크 공유로 의존성 관리

2. **Java 기반 Kafka Streams 처리 로직**
   - 역할: user_events 실시간 분기, alert_events 세부 이벤트 Slack 알림
   - 핵심: branch() API 활용, 토픽 분리 및 브랜치 재활용
   - 효과: 시스템 부하 감소, 장애 대응력 향상

3. **맞춤형 마케팅 알림 기능 (Slack 연동)**
   - 역할: 이벤트 분석 후 Slack 채널로 메시지 전송
   - 구현: SlackBlockBuilder, SlackNotifier 활용
   - 효과: 실시간 마케팅 메시지 전달 및 테스트 효율 향상

4. **Python 기반 Data Generator 및 제어 스크립트**
   - 역할: 유저 행동 이벤트 시뮬레이션, Kafka 토픽 전송, DB 적재, 취소 이벤트 생성
   - 핵심: schedule/random/json 활용, .config 외부 제어, Airflow DAG 연동
   - 효과: 실제 사용자 시나리오 기반 테스트 데이터 확보

5. **Kafka Connect 및 CDC 파이프라인**
   - 역할:
     - Cassandra Sink: alert/order 이벤트 실시간 적재
     - PostgreSQL Sink: order_events DB 적재
     - Debezium Source: PostgreSQL 변경 감지
     - BigQuery Sink: CDC 데이터 적재
   - 효과: CDC 기반 실시간 데이터 동기화, 데이터 손실 최소화, 분석 자동화

6. **모니터링 시스템**
   - 구성 요소: Prometheus, cAdvisor, Grafana
   - 구현: 각 타깃 수집 설정, Grafana 대시보드 자동 세팅
   - 효과: 실시간 처리량, 지연율, 데이터 적재 정확도 모니터링 가능

---

## 5. 성과 및 개선점

- **성과**
  - 실시간 데이터 파이프라인 구축 (Postgres → Kafka → BigQuery)
  - 비정형 로그 실시간 적재 및 시계열 분석 가능
  - Kafka Streams 분기 기반 알림 자동화
  - Prometheus/Grafana로 실시간 모니터링
  - 데이터 안정성 및 무결성 확보

- **개선점**
  - Kafka Connector 클러스터링 및 브로커 다중화 필요
  - Prometheus/Grafana 알림 룰 고도화, Airflow 자동화
  - 스키마 진화 관리, 데이터 정합성 체크 자동화
  - Slack 외 다른 채널 알림 확장, 실시간 추천/마케팅 활용

---

## 6. 참고

- **KPI 달성 여부**

| 지표명 | 측정 기준 | 목표 | 측정 수치 |
| --- | --- | --- | --- |
| 이벤트 처리 지연 시간 | Kafka 프로듀싱 → 소비까지 평균 시간 | 2초 이내 | 최장 1.40초 |
| 메시지 처리 속도 | 메시지 발생 속도 초당 처리 수 | 2건 이상 | 평균 2.4건, 최고 4.62건 |
| CDC 전송 지연 | Postgres → BigQuery 반영 시간 | 5초 이내 | 평균 3.565초 |
| 알림 반응 시간 | 이벤트 발생 → Slack 수신 | 3초 이내 | 평균 2.372초 |
| 데이터 적재 정확도 | CDC 후 BigQuery 레코드 일치율 | 99.5% 이상 | 99.9% |

- KPI 관련 그래프 예시
  - 이벤트 처리 지연 시간, 메시지 처리 속도, CDC 전송 지연, 알림 반응 시간, 데이터 적재 정확도 그래프 포함
