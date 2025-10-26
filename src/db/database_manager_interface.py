from abc import ABC, abstractmethod
from typing import Optional
from psycopg2.extensions import connection
from src.db.postgres_manager import QueryType


class IDatabaseManager(ABC):
    """
    An interface for database operation management.

    This interface defines methods for managing database entities and peforming
    standard operations on entities.
    """
    @abstractmethod
    def drop_table(self):
        """
        Drop a database table entity.
        """
        pass

    @abstractmethod
    def create_table(self, conn: connection):
        """
        Create a database table entity.
        """
        pass

    @abstractmethod
    def execute_read(self,  query_type: QueryType, conn: connection, params: Optional[tuple]):
        """
        Performs select operation on provided table entity.
        """
        pass

    @abstractmethod
    def execute_write(self, query_type: QueryType, conn: connection, params: Optional[tuple]):
        """
        Performs select operation on provided table entity.
        """
        pass
