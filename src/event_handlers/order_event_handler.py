from logging import Logger
from typing import Any, List
from watchdog.events import PatternMatchingEventHandler
from db.database_interface import IDatabase
from db.db_context_manager import ManagedConnection
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

    def on_created(self, event: Any) -> None:
        """
        Handles the processing of new raw files.

        This method is triggered when a raw CSV file is created in the watched directory.
        It processes the file using the IProcessor, and manages database connections
        using a context manager to ensure proper connection cleanup.
        """
        if event:
            try:
                with ManagedConnection(self.db_conn) as conn:
                    self.processor.process_file(event.src_path, conn)
            except Exception as e:
                self.logger.error(f"Error processing file {event.src_path}: {e}")
