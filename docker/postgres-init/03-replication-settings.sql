-- CDC/Replication을 위한 PostgreSQL 설정
-- wal_level = logical, replication 슬롯 수, 동시 연결 수 등

-- wal_level 설정
ALTER SYSTEM SET wal_level = logical;

-- max replication slots 설정
ALTER SYSTEM SET max_replication_slots = 10;

-- max wal senders 설정
ALTER SYSTEM SET max_wal_senders = 10;

-- 설정 적용을 위해 서버 재시작 필요
-- 컨테이너 초기화 시 자동 적용됨
