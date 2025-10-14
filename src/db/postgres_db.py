import psycopg2
import psycopg2.pool
import threading
from db.database_interface import IDatabase


class PostgresDB(IDatabase):
    """
    A PostgreSQL database client with connection pooling.

    This class manages a connection pool using psycopg2's ThreadedConnectionPool
    to provide efficient and thread-safe database connections.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """
        Assists with the implementation of Singleton Pattern to ensure only a single instance of DB is used.
        """
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(PostgresDB, cls).__new__(cls)
        return cls._instance

    def __init__(self, dbname, user, password, host, port, min_conn, max_conn, logger):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self.logger = logger
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.logger.info("Connection Pool initialised.")
        self._initialized = True

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            raise ValueError("PostgresDB not initialized. Create an instance first.")
        return cls._instance

    def get_connection(self):
        self.logger.info("Fetching Connection")
        if self._pool:
            return self._pool.getconn()
        raise ConnectionError("Pool not initialised.")

    def release_connection(self, conn):
        self.logger.info("Releasing Connection")
        if self._pool:
            self._pool.putconn(conn)

    def close_pool(self):
        if self._pool:
            self._pool.closeall()
            self._pool = None
            self.logger.info("Connection Pool closed.")
            self._initialized = False
