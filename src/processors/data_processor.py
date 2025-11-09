import os
from logging import Logger
from datetime import datetime
from typing import Any
from sqlalchemy import column, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Connection
from db.postgres_manager import PostgresManager
from db.query_types import QueryReturnType, QueryType
from utils.utils import get_md5, extract_file_name
from processors.processor_interface import IProcessor
from processors.processor_config import ProcessorConfig


class PostgresDataProcessor(IProcessor):
    """
    Generic data processor for handling CSV file ingestion into PostgreSQL.

    This class provides a configurable, reusable implementation for processing
    different types of data. Entity-specific details are provided via ProcessorConfig.

    The processor handles:
    - Dropping and creating necessary tables
    - Deduplication using MD5 digest tracking
    - Inserting data from CSV files into temporary tables
    - Merging data from temporary tables into target tables with upsert logic
    - Maintaining a manifest of processed files

    Args:
        config: ProcessorConfig containing entity-specific table and metadata information
        database_manager: PostgresManager for executing database operations
        logger: Logger for tracking processing steps and errors
    """
    def __init__(self,
                 config: ProcessorConfig,
                 database_manager: PostgresManager,
                 logger: Logger) -> None:

        self.config: ProcessorConfig = config
        self.database_manager: PostgresManager = database_manager
        self.logger: Logger = logger

    def generate_manifest_fields(self, file: str) -> dict[str, Any]:
        """
        Generates a manifest dictionary containing metadata about a file.

        This method extracts the file name, calculates its MD5 digest, retrieves its size,
        and records the time of processing. The resulting manifest is returned as a dictionary.

        Args:
            file: Path to the file being processed

        Returns:
            Dictionary containing file_name, digest, file_size, and processed_at timestamp
        """
        file_name = extract_file_name(file)
        digest = get_md5(file)
        file_size = os.path.getsize(file)
        processed_at = datetime.now().timestamp()
        manifest = {
            "file_name": file_name,
            "digest": digest,
            "file_size": file_size,
            "processed_at": processed_at
            }

        return manifest

    def set_up_tables(self) -> None:
        """
        Drops and creates tables in the database.

        This method drops the temporary table and then creates both the temporary
        and manifest tables for the configured entity.
        """
        self.logger.info(f"Dropping and Creating {self.config.entity_name} tables...")
        self.database_manager.drop_table(self.config.tmp_metadata)
        self.database_manager.create_table(self.config.tmp_metadata)
        self.database_manager.create_table(self.config.manifest_metadata)

    def process_file(self, csv_file: str, conn: Connection) -> None:
        """
        Processes a CSV file by performing a series of actions to update the database.

        This method checks if the file's digest is already present in the manifest table.
        If the digest is not found, it processes the file by:
        1. Copying data from the CSV file into the temporary table.
        2. Generating the manifest fields for the file.
        3. Inserting the manifest data into the manifest table.
        4. Merging the data from the temporary table into the main target table.

        If any exceptions are raised during the process, they are logged for further investigation.

        Args:
            csv_file: Path to the CSV file to process
            conn: Active SQLAlchemy database connection
        """
        self.logger.info("Generating Digest...")
        digest = get_md5(csv_file)
        self.logger.info(f"MD5: {digest}")

        digest_query_stm = select(
            select(text("1"))
            .select_from(self.config.manifest_table)
            .where(column("digest") == digest)
            .exists()
        )

        digest_query = QueryType(
            name=f"{self.config.entity_name}_digest_query",
            sql=digest_query_stm,
            return_type=QueryReturnType.SCALAR
        )

        results = self.database_manager.execute_read(
            query_type=digest_query,
            conn=conn
            )

        if not results[0]:
            try:
                self.logger.info("Processing new batch...")
                self.database_manager.execute_csv_copy(self.config.tmp_table, csv_file, conn)
                manifest_cols = self.generate_manifest_fields(csv_file)
                self.insert_to_table(self.config.manifest_table, manifest_cols, conn)
                self.merge_tables(self.config.tmp_table, self.config.target_table, conn)
            except Exception:
                self.logger.error("Failure Occurred: ", exc_info=True)

        else:
            self.logger.info("Batch already processed")

    def insert_to_table(self, table_name: str, columns: dict[str:str], conn: Connection) -> None:
        """
        Inserts a row into the specified table.

        Args:
            table_name: Table to insert into
            columns: Dictionary of column names and values
            conn: Active SQLAlchemy database connection
        """
        self.logger.info(f"Inserting to {table_name}")
        insert_stm = insert(table_name).values(columns)

        insert_query = QueryType(
            name=f"{self.config.entity_name}_insert_query",
            sql=insert_stm,
            return_type=QueryReturnType.NONE
        )

        self.database_manager.execute_write(
            query_type=insert_query,
            conn=conn
        )

    def merge_tables(self, tmp_table: str, target_table: str, conn: Connection) -> None:
        """
        Performs an upsert operation by merging data from a temporary table into a target table.

        This method reads all records from a temporary staging table and inserts them into
        the target table. If a row with the same primary key already exists in the target table,
        the existing row is updated only if the corresponding record in the temporary table
        has a more recent `processed_at` timestamp.

        The merge operation is executed using a SQLAlchemy `INSERT ... ON CONFLICT DO UPDATE`
        statement to ensure data consistency while avoiding duplicate primary key violations.

        Args:
            tmp_table: Temporary table containing staged data
            target_table: Destination table to merge into
            conn: Active SQLAlchemy database connection

        Behavior:
            - Reads all records from tmp_table
            - Inserts new records into target_table
            - Updates existing records only when the processed_at timestamp
              in the source (temporary) table is more recent
            - Logs intermediate queries and execution results for debugging
        """
        self.logger.info(f"Merging into {target_table}")
        select_tmp_stm = select(text("*")).select_from(tmp_table)

        select_tmp_query = QueryType(
            name=f"{self.config.entity_name}_select_tmp_query",
            sql=select_tmp_stm,
            return_type=QueryReturnType.ALL
        )

        results = self.database_manager.execute_read(
            query_type=select_tmp_query,
            conn=conn
        )
        self.logger.info("TMP QUERY RESULTS: %s" % results)

        insert_stmt = (
            insert(target_table)
            .from_select(
                [col.name for col in self.config.tmp_table.columns],
                select(self.config.tmp_table)
            )
        )

        set_clause = {
            c.name: getattr(insert_stmt.excluded, c.name)
            for c in target_table.columns
            if not c.primary_key
        }

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[self.config.primary_key_column],
            set_=set_clause,
            where=insert_stmt.excluded.processed_at > target_table.c.processed_at
        )

        insert_target_query = QueryType(
            name="merge_tmp_into_main",
            sql=upsert_stmt,
            return_type=QueryReturnType.NONE
        )

        self.database_manager.execute_write(
            query_type=insert_target_query,
            conn=conn
        )
