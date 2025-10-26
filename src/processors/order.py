import os
from logging import Logger
from datetime import datetime
from sqlalchemy import column, select, text
from sqlalchemy.dialects.postgresql import insert
from db.postgres_manager import PostgresManager, QueryReturnType, QueryType
from utils.utils import get_md5, extract_file_name
from processors.processor_interface import IProcessor
from sqlalchemy import Connection
from models.order import tmp_metadata, manifest_metadata
from models.order import tmp_order, order_manifest, order


class OrderProcessor(IProcessor):
    """
    OrderProcessor processes raw order data within a database.

    This class interacts with various components for managing tables, reading from tables, inserting and merging data,
    and logging information regarding the processing steps. The main operations include:
        - Dropping and creating necessary tables.
        - Inserting data from orders CSV files into temporary tables and the main order tables.
        - Merging data from temporary tables into the main order table.

    The class ensures that the data processing is handled in a structured and logged manner, providing transparency
    in case of failures or issues during processing.
    """
    def __init__(self,
                 database_manager: PostgresManager,
                 logger: Logger):

        self.database_manager = database_manager
        self.logger = logger
        self.tmp_metadata = tmp_metadata
        self.manifest_metadata = manifest_metadata
        self.tmp_order = tmp_order
        self.order_manifest = order_manifest
        self.order = order

    def generate_manifest_fields(self, file):
        """
        Generates a manifest dictionary containing metadata about a file.

        This method extracts the file name, calculates its MD5 digest, retrieves its size,
        and records the time of processing. The resulting manifest is returned as a dictionary.
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

    def set_up_tables(self, engine):
        """
        Drops and creates tables in the database.

        This method drops the temporary table and then creates both the temporary
        and manifest tables using the provided sqlalchemy engine.
        """
        self.logger.info("Dropping and Creating tables...")
        self.database_manager.drop_table(tmp_metadata, engine)
        self.database_manager.create_table(tmp_metadata, engine)
        self.database_manager.create_table(manifest_metadata, engine)

    def process_file(self, csv_file, conn: Connection):
        """
        Processes the orders CSV file by performing a series of actions to update the database.

        This method checks if the file's digest is already present in the order_manifest table.
        If the digest is not found, it processes the file by:
        1. Copying data from the CSV file into the temporary table `tmp_order`.
        2. Generating the manifest fields for the file.
        3. Inserting the manifest data into the `order_manifest` table.
        4. Merging the data from the temporary table `tmp_order` into the main `order` table.

        If any exceptions are raised during the process, they are logged for further investigation.
        """
        self.logger.info("Generating Digest...")
        digest = get_md5(csv_file)
        self.logger.info(f"MD5: {digest}")

        digest_query_stm = (
            select(text("1"))
            .select_from(self.order_manifest)
            .where(column("digest") == digest)
            .exists()
        )

        digest_query = QueryType(
            name="orders_digest_query",
            sql=digest_query_stm,
            return_type=QueryReturnType.SCALAR
        )

        results = self.database_manager.execute_read(
            query_type=digest_query,
            conn=conn
            )

        if results is not None:
            self.logger.error("Error Occurred reading from %s " % self.order_manifest)
        
        elif not results:
            try:
                self.logger.info("Processing new batch...")
                self.database_manager.execute_csv_copy(self.tmp_order, csv_file, conn)
                manifest_cols = self.generate_manifest_fields(csv_file)
                self.insert_to_table(self.order_manifest, manifest_cols, conn)
                self.merge_tables(self.tmp_order, self.order, conn)
            except Exception:
                self.logger.error("Failure Occurred: ", exc_info=True)

        else:
            self.logger.info("Batch already processed")

    def insert_to_table(self, table_name, columns: dict[str:str], conn: Connection):
        """
        Inserts a row into the specified table.
        """
        self.logger.info(f"Inserting to {table_name}")
        insert_stm = insert(table_name).values(columns)

        order_insert_query = QueryType(
            name="orders_insert_query",
            sql=insert_stm,
            return_type=QueryReturnType.NONE
        )

        self.database_manager.execute_write(
            query_type=order_insert_query,
            conn=conn
        )

    def merge_tables(self, tmp_table, target_table, conn: Connection):
        """
        Merges data from a temporary table into the main table,
        avoiding duplicate primary key values.
        """
        self.logger.info(f"Merging into {target_table}")
        # TODO update to insert where processed_at on tmp is greater than target
        select_tmp_stm = select(text("*")).select_from(tmp_table)

        select_tmp_query = QueryType(
            name="orders_select_tmp_query",
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
                [col.name for col in tmp_order.columns],
                select(tmp_order)
            )
        )

        set_clause = {
            c.name: getattr(insert_stmt.excluded, c.name)
            for c in target_table.columns
            if not c.primary_key
        }

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['order_id'],
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
