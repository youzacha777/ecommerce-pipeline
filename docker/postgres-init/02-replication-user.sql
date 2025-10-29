-- 퍼블리케이션 수동 생성 (슈퍼유저 권한 안주기 위해)
CREATE PUBLICATION debezium_pub FOR ALL TABLES;

-- CDC/Replication 전용 계정 생성
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD 'dbzpassword';

-- event_db에 접속 권한
GRANT CONNECT ON DATABASE event_db TO debezium;

-- 퍼블리케이션 생성 권한 부여
GRANT CREATE ON DATABASE event_db TO debezium;

-- public 스키마 사용 권한
GRANT USAGE ON SCHEMA public TO debezium;

-- 테이블 SELECT 권한
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- 이후 새로 생성될 테이블에도 자동으로 SELECT 권한 부여
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO debezium;

-- 시퀀스 객체에 대한 권한 부여
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO debezium;
