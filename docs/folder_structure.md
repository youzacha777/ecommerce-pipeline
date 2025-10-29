
```
ecommerce-pipeline
├─ airflow
│  └─ dags
│     ├─ cancel_event_dag.py
│     ├─ generate_event_dag.py
│     └─ __pycache__
│        ├─ cancel_event_dag.cpython-38.pyc
│        └─ generate_event_dag.cpython-38.pyc
├─ connect-distributed.properties
├─ docker
│  ├─ cassandra-init
│  │  ├─ 00-init-db.sh
│  │  ├─ 01-create-keyspace.cql
│  │  ├─ 02-create-alert-events.cql
│  │  └─ 03-create-order-events.cql
│  ├─ connectors
│  │  ├─ alert-events-cassandra-sink.json
│  │  ├─ bigquery-sink.json
│  │  ├─ entrypoint.sh
│  │  ├─ init-connectors.sh
│  │  ├─ order-events-cassandra-sink.json
│  │  └─ postgres-debezium-source.json
│  ├─ docker-compose.yml
│  ├─ Dockerfile.airflow
│  ├─ Dockerfile.cancel_generator
│  ├─ Dockerfile.connect
│  ├─ Dockerfile.custom_exporter
│  ├─ Dockerfile.event_generator
│  ├─ Dockerfile.java_streams
│  ├─ Dockerfile.main_alert_addcart_consumer
│  ├─ Dockerfile.main_save_addcart_consumer
│  ├─ Dockerfile.main_save_purchase_consumer
│  ├─ grafana
│  │  ├─ dashboards
│  │  │  ├─ kafka_topic_dashboard.json
│  │  │  ├─ metric_monioring_dashboard.json
│  │  │  └─ node_cadvisor_dashboard.json
│  │  └─ provisioning
│  │     ├─ dashboards
│  │     │  └─ dashboard.yml
│  │     └─ datasources
│  │        └─ datasources.yml
│  ├─ kafka-init
│  │  └─ create-topics.sh
│  ├─ postgres-init
│  │  ├─ 00-init-db.sh
│  │  ├─ 01-create-tables.sql
│  │  ├─ 02-replication-user.sql
│  │  └─ 03-replication-settings.sql
│  ├─ prometheus
│  │  ├─ custom_exporter.py
│  │  └─ prometheus.yml
│  └─ register-connector.sh
├─ flags
├─ java_src
│  ├─ dependency-reduced-pom.xml
│  ├─ pom.xml
│  ├─ src
│  │  └─ main
│  │     ├─ java
│  │     │  ├─ notifiers
│  │     │  │  ├─ SlackBlockBuilder.java
│  │     │  │  └─ SlackNotifier.java
│  │     │  ├─ streams
│  │     │  │  └─ UserEventStreamProcessor.java
│  │     │  └─ utils
│  │     │     ├─ ProductDTO.java
│  │     │     └─ ProductLoader.java
│  │     └─ resources
│  │        ├─ products.json
│  │        └─ streams_config.properties
│  ├─ state
│  └─ target
│     ├─ classes
│     │  ├─ notifiers
│     │  │  ├─ SlackBlockBuilder.class
│     │  │  └─ SlackNotifier.class
│     │  ├─ products.json
│     │  ├─ streams
│     │  │  ├─ UserEventStreamProcessor$EventType.class
│     │  │  └─ UserEventStreamProcessor.class
│     │  ├─ streams_config.properties
│     │  └─ utils
│     │     ├─ ProductDTO.class
│     │     ├─ ProductLoader$1.class
│     │     └─ ProductLoader.class
│     ├─ generated-sources
│     │  └─ annotations
│     ├─ maven-archiver
│     │  └─ pom.properties
│     ├─ maven-status
│     │  └─ maven-compiler-plugin
│     │     └─ compile
│     │        └─ default-compile
│     │           ├─ createdFiles.lst
│     │           └─ inputFiles.lst
│     ├─ original-user-event-streams-1.0-SNAPSHOT.jar
│     ├─ test-classes
│     └─ user-event-streams-1.0-SNAPSHOT.jar
├─ postgresql.conf
├─ python_src
│  ├─ config
│  │  ├─ config.py
│  │  ├─ users.json
│  │  ├─ user_generator.py
│  │  └─ __pycache__
│  │     └─ config.cpython-39.pyc
│  ├─ event_generator
│  │  ├─ cancel_generator.py
│  │  ├─ event_generator.py
│  │  └─ __pycache__
│  │     ├─ cancel_generator.cpython-38.pyc
│  │     └─ cancel_generator.cpython-39.pyc
│  ├─ flags
│  ├─ main
│  │  ├─ main_alert_addtocart.py
│  │  ├─ main_save_addtocart.py
│  │  └─ main_save_purchase.py
│  ├─ notifier
│  │  ├─ slack_notifier.py
│  │  └─ __pycache__
│  │     └─ slack_notifier.cpython-39.pyc
│  ├─ processors
│  │  ├─ addtocart_db_processor.py
│  │  ├─ purchase_db_processor.py
│  │  └─ __pycache__
│  │     ├─ addtocart_db_processor.cpython-39.pyc
│  │     └─ purchase_db_processor.cpython-39.pyc
│  └─ utils
│     ├─ kafka_utils.py
│     ├─ log_utils.py
│     ├─ postgres_conn_utils.py
│     ├─ target
│     │  ├─ classes
│     │  ├─ maven-archiver
│     │  │  └─ pom.properties
│     │  ├─ slack-test-1.0-SNAPSHOT.jar
│     │  └─ test-classes
│     └─ __pycache__
│        ├─ kafka_utils.cpython-39.pyc
│        ├─ log_utils.cpython-39.pyc
│        └─ postgres_conn_utils.cpython-39.pyc
└─ requirements.txt

```