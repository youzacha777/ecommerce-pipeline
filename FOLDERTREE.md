## 프로젝트 구조 요약 

ecommerce-pipeline/
├─ airflow/ # Airflow DAG 관리
├─ docker/ # Docker 환경 설정
│ ├─ connectors/ # Kafka Connect 커넥터 설정
│ ├─ grafana/ # Grafana 대시보드 및 데이터소스 설정
│ ├─ kafka-init/ # Kafka 초기화 스크립트
│ ├─ postgres-init/ # PostgreSQL 초기화 스크립트
│ └─ prometheus/ # Prometheus 설정
├─ java_src/ # Java 기반 Kafka Streams 로직
├─ python_src/ # Python 이벤트 생성 및 알림 로직
└─ requirements.txt # Python 패키지 의존성

전체 폴더 구조 보기 → [docs/folder_structure.md](docs/folder_structure.md)