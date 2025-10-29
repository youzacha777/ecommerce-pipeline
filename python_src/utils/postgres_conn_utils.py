import psycopg2
import logging
from config.config import POSTGRES_CONFIG
from utils.log_utils import setup_logger

logger = setup_logger("postgres_conn_utils", "logs/postgres_conn_utils.log")

def get_connection():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.autocommit = True
        logger.info("PostgreSQL 연결 성공")
        return conn
    except Exception as e:
        logger.error(f"PostgreSQL 연결 실패: {e}")
        return None

def close_connection(conn):
    if conn:
        conn.close()
        logger.info("PostgreSQL 연결 종료")