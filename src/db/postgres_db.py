from logging import Logger
from typing import Optional
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import connection
from threading import Lock, Semaphore
from db.database_interface import IDatabase
from db.db_context_manager import ManagedConnection


class PostgresDB(IDatabase):
    """
    Thread-safe PostgreSQL client with connection pooling and singleton pattern.

    This class uses psycopg2's ThreadedConnectionPool to manage database connections efficiently.
    A semaphore enforces a maximum number of concurrent connections, preventing connection starvation
    when multiple threads request connections simultaneously.
    """
    _instance: Optional["PostgresDB"] = None
    _lock: Lock = Lock()

    def __new__(cls, *args, **kwargs) -> "PostgresDB":
        """
        Singleton implementation using double-checked locking for thread safety.

        Returns:
            PostgresDB: The single instance of this class.
        """
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self,
                 dbname: str,
                 user: str,
                 password: str,
                 host: str,
                 port: int,
                 min_conn: int,
                 max_conn: int,
                 logger: Logger) -> None:
        """
        Initialize the connection pool and semaphore. Ensures initialization occurs only once.

        Args:
            dbname (str): Database name.
            user (str): Database user.
            password (str): User password.
            host (str): Database host.
            port (int): Database port.
            min_conn (int): Minimum connections in the pool.
            max_conn (int): Maximum connections in the pool.
            logger: Logger instance for logging pool operations.
        """
        if getattr(self, "_initialized", False):
            return

        # Store connection details for repr and debugging
        self.dbname: str = dbname
        self.user: str = user
        self.host: str = host
        self.port: int = port
        # Semaphore limits concurrent access to max_conn
        self._semaphore: Semaphore = Semaphore(max_conn)
        self.logger: Logger = logger
        self._pool: Optional[ThreadedConnectionPool] = ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self._initialized: bool = True

        self.logger.info("Connection Pool initialised.")

    def __repr__(self):
        return f"PostgresDB(dbname={self.dbname}, user={self.user}, host={self.host}, port={self.port})"

    @classmethod
    def get_instance(cls) -> "PostgresDB":
        """
        Retrieve the singleton instance.

        Raises:
            ValueError: If the singleton has not been initialized yet.
        """
        if not cls._instance:
            raise ValueError("PostgresDB not initialized. Create an instance first.")
        return cls._instance

    def get_connection(self) -> ManagedConnection:
        """
        Acquire a connection from the pool. Blocks if max concurrent connections are in use.

        Returns:
            connection: A psycopg2 connection object.

        Raises:
            ConnectionError: If the pool is not initialized.
        """
        self.logger.info("Fetching Connection")
        if not self._pool:
            raise ConnectionError("Pool not initialised.")

        self._semaphore.acquire()
        try:
            return self._pool.getconn()
        except Exception:
            self._semaphore.release()
            raise

    def release_connection(self, conn: connection) -> None:
        """
        Release a connection back to the pool and free a semaphore permit.

        Args:
            conn: The connection object to release.
        """
        self.logger.info("Releasing Connection")
        if self._pool:
            try:
                self._pool.putconn(conn)
            except Exception:
                raise
            finally:
                self._semaphore.release()

    def close_pool(self) -> None:
        """
        Close all connections and mark the pool as uninitialized.
        """
        if self._pool:
            with self._lock:
                try:
                    self._pool.closeall()
                    self.logger.info("Connection Pool closed.")
                except Exception:
                    raise
                finally:
                    self._pool = None
                    self._initialized = False
