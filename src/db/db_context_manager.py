from typing import Any
from src.db.database_interface import IDatabase


class ManagedConnection:
    """Context manager for automatic connection release on DB instances."""

    def __init__(self, db: IDatabase):
        self.db = db
        self.conn = None

    def __enter__(self) -> Any:
        self.conn = self.db.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self.conn:
            self.db.release_connection(self.conn)
        return False
