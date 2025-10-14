from abc import ABC, abstractmethod


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
    def create_table(self, conn):
        """
        Create a database table entity.
        """
        pass

    @abstractmethod
    def execute_read(self, query_type, conn, params):
        """
        Performs select operation on provided table entity.
        """
        pass

    @abstractmethod
    def execute_write(self, query_type, conn, params):
        """
        Performs select operation on provided table entity.
        """
        pass
