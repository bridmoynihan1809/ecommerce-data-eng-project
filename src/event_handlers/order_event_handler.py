from logging import Logger
from typing import List
from watchdog.events import PatternMatchingEventHandler
from db.database_interface import IDatabase
from event_handlers.event_handler_interface import IEventHandler
from processors.processor_interface import IProcessor
# from processors.order import OrderProcessor


class OrderEventHandler(IEventHandler, PatternMatchingEventHandler):
    """
    Handles events triggered by file changes for raw order files.

    This class extends the PatternMatchingEventHandler from watchdog to specifically handle `.csv` files
    related to order events.
    It processes the created files by invoking the appropriate methods in the orderProcessor
    and interacts with the PostgresDB instance to manage database connections.
    """
    def __init__(self, processor: IProcessor, db_conn: IDatabase, patterns: List[str], logger: Logger):
        self.processor = processor
        self.db_conn = db_conn
        self.logger = logger
        PatternMatchingEventHandler.__init__(self, patterns=patterns, ignore_directories=True)

    def on_created(self, event):
        """
        Handles the processing of new raw files.

        This method is triggered when a raw CSV file is created in the watched directory.
        It processes the file using the IProcessor, and manages database connections
        by acquiring a connection, using it to process the file, and then releasing the connection.
        """
        if event:
            try:
                conn = self.db_conn.get_connection()
                self.processor.process_file(event.src_path, conn)
            except Exception as e:
                self.logger.error(f"Error connecting {e}")
            finally:
                if self.db_conn:
                    self.db_conn.release_connection(conn)
