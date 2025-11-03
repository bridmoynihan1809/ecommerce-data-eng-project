from abc import ABC, abstractmethod
from typing import Any


class IDatabase(ABC):
    """
    An interface for database connection management.

    This interface defines methods for obtaining and releasing database connections,
    as well as closing connection pools.
    """
    @abstractmethod
    def get_connection(self) -> Any:
        """
        Retrieves a database connection from the connection pool.
        """
        pass

    @abstractmethod
    def release_connection(self, conn: Any) -> None:
        """
        Releases a previously acquired database connection back to the pool."
        """
        pass

    @abstractmethod
    def close_pool(self) -> None:
        """
        Closes the database connection pool.
        """
        pass
