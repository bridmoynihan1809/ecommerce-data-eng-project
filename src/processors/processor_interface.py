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
    def set_up_tables(self):
        """
        Sets up the necessary tables within the database
        """
        pass

    @abstractmethod
    def generate_manifest_fields(self, file: str):
        """
        Generates a set of manifest fields for a given file.
        """
        pass

    @abstractmethod
    def process_file(self, file_path: str, conn: Connection):
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
    def merge_tables(self, tmp_table: str, target_table: str, conn: Connection):
        """
        Merges data from a temporary table into the main table,
        avoiding duplicate primary key values.
        """
        pass
