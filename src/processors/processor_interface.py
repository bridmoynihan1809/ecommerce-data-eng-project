from abc import ABC, abstractmethod
from sqlite3 import Connection


class IProcessor(ABC):
    """
    Abstract base class defining the interface for processing data within a specific domain.

    This interface establishes the required methods for processors that manage and manipulate data.
    Concrete implementations of this interface will define the actual behavior for setting up tables,
    generating manifest fields, and processing raw files.

    """
    @abstractmethod
    def set_up_tables(self, engine):
        """
        Sets up the necessary tables within the database using the provided engine.
        """
        pass

    @abstractmethod
    def generate_manifest_fields(self, file):
        """
        Generates a set of manifest fields for a given file.
        """
        pass

    @abstractmethod
    def process_file(self, file_path, conn):
        """
        Processes the file at the specified path and applies necessary operations to the database connection.
        """
        pass

    @abstractmethod
    def insert_to_table(self, table_name, columns: dict[str:str], conn: Connection):
        """
        Inserts a row into the specified table.
        """
        pass
    
    @abstractmethod
    def merge_tables(self, tmp_table, target_table, conn: Connection):
        """
        Merges data from a temporary table into the main table,
        avoiding duplicate primary key values.
        """
        pass
