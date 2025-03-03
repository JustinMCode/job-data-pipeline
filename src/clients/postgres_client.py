import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import DatabaseError
from src.utils.config import (
    DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT,
    POOL_MIN_CONN, POOL_MAX_CONN, CONNECTION_TIMEOUT
)
from src.utils.logger import logger

class PostgresClient:
    _pool = None

    @classmethod
    def initialize_pool(cls):
        if cls._pool is None:
            try:
                cls._pool = SimpleConnectionPool(
                    POOL_MIN_CONN,
                    POOL_MAX_CONN,
                    host=DB_HOST,
                    dbname=DB_NAME,
                    user=DB_USER,
                    password=DB_PASS,
                    port=DB_PORT,
                    connect_timeout=CONNECTION_TIMEOUT,
                    sslmode="require"
                )
                logger.info("PostgreSQL connection pool initialized")
            except Exception as e:
                logger.error("Error initializing connection pool", exc_info=True)
                raise

    @classmethod
    def get_connection(cls):
        if cls._pool is None:
            cls.initialize_pool()
        return cls._pool.getconn()

    @classmethod
    def release_connection(cls, conn):
        if cls._pool:
            cls._pool.putconn(conn)

    @classmethod
    def close_all_connections(cls):
        if cls._pool:
            cls._pool.closeall()