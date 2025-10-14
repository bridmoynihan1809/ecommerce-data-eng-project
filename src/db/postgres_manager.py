from dataclasses import dataclass
from enum import Enum
from sqlite3 import Connection
from typing import Any, Dict, List, Optional
import psycopg2
import psycopg2.extras
from sqlalchemy.dialects import postgresql

from db.database_manager_interface import IDatabaseManager

QueryResult = List[Dict[str, Any]]


class QueryReturnType(Enum):
    SCALAR = "scalar"
    ONE = "one"
    ALL = "all"
    NONE = None


@dataclass(frozen=True)
class QueryType:
    name: str
    sql: str
    return_type: QueryReturnType


class PostgresManager(IDatabaseManager):
    def __init__(self, logger):
        self.logger = logger

    def drop_table(self, table_metadata, engine):
        """
        Drops tables referenced in the table metadata passed.
        """
        self.logger.info("Dropping tables")
        table_metadata.drop_all(bind=engine)

    def create_table(self, table_metadata, engine):
        """
        Creates tables referenced in the table metadata passed.
        """
        self.logger.info("Creating tables")
        table_metadata.create_all(bind=engine)
    
    def execute_csv_copy(self, table_name, csv_file, conn: Connection):
        """
        Bulk inserts data into a table from a CSV file.
        """
        copy_columns = [col.name for col in table_name.columns if col.server_default is None]
        columns_str = ", ".join(copy_columns)
        with conn.cursor() as curr:
            copy_from = f"COPY {table_name}({columns_str}) FROM STDIN WITH CSV HEADER NULL AS 'NULL';"
            with open(csv_file, 'r') as file:
                try:
                    curr.copy_expert(copy_from, file)
                except Exception:
                    # TODO if exception here prevent process from proceeding (TRY/CATCH)
                    self.logger.error("Copy CSV Error Occurred:", exc_info=True)
            conn.commit()

    def execute_write(
        self,
        query_type: QueryType,
        conn: Connection,
        params: Optional[tuple] = None
    ) -> None:
        self.logger.info(f"Running query: {query_type.name} | SQL: {query_type.sql}")

        try:
            raw_sql = query_type.sql.compile(
                            dialect=postgresql.dialect(),
                            compile_kwargs={"literal_binds": True}
                        ).string
        except Exception:
            self.logger.error("Compilation Error Occurred:", exc_info=True)

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
            try:
                curr.execute(raw_sql, params)
                conn.commit()
                self.logger.info(f"Rows Inserted: {curr.rowcount}")
            except Exception:
                self.logger.error("Error Occurred:", exc_info=True)
                self.logger.info("Rolling back transaction")
                conn.rollback()
                self.logger.info("Transaction rolled back")

    def execute_read(
        self,
        query_type: QueryType,
        conn: Connection,
        params: Optional[tuple] = None
    ) -> QueryResult:
        self.logger.info(f"Running query: {query_type.name} | SQL: {query_type.sql}")

        try:
            raw_sql = query_type.sql.compile(
                            dialect=postgresql.dialect(),
                            compile_kwargs={"literal_binds": True}
                        ).string
        except Exception:
            self.logger.error("Compilation Error Occurred:", exc_info=True)

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curr:
            try:
                self.logger.info(f"Executing: {raw_sql}")
                curr.execute(raw_sql, params)

                if query_type.return_type.value == "scalar":
                    result = curr.fetchone()
                    return result if result else []

                elif query_type.return_type.value == "one":
                    result = curr.fetchone()
                    return result if result else []

                elif query_type.return_type.value == "all":
                    result = curr.fetchall()
                    return result if result else []
                else:
                    raise ValueError(f"Unknown return_type: {query_type.return_type}")

            except Exception:
                self.logger.error("Error Occurred:", exc_info=True)
