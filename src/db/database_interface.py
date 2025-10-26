from abc import ABC, abstractmethod
from psycopg2.extensions import connection


class IDatabase(ABC):
    """
    An interface for database connection management.

    This interface defines methods for obtaining and releasing database connections,
    as well as closing connection pools.
    """
    @abstractmethod
    def get_connection(self):
        """
        Retrieves a database connection from the connection pool.
        """
        pass

    @abstractmethod
    def release_connection(self, conn: connection):
        """
        Releases a previously acquired database connection back to the pool."
        """
        pass

    @abstractmethod
    def close_pool(self, conn: connection):
        """
        Closes the database connection pool.
        """
        pass
