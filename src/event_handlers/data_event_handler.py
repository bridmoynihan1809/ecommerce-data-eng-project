from logging import Logger
from typing import Any, List
from watchdog.events import PatternMatchingEventHandler
from db.database_interface import IDatabase
from db.db_context_manager import ManagedConnection
from event_handlers.event_handler_interface import IEventHandler
from processors.processor_interface import IProcessor


class DataEventHandler(IEventHandler, PatternMatchingEventHandler):
    """
    Generic event handler for file change events.

    This class extends PatternMatchingEventHandler from watchdog to handle file events
    for any type of data entity. It processes the created files
    by invoking the appropriate processor and manages database connections.

    This replaces entity-specific event handlers (OrderEventHandler, CustomerEventHandler, etc.)
    with a single configurable implementation.

    Args:
        processor: IProcessor instance for processing the files
        db_conn: IDatabase instance for managing database connections
        patterns: List of file patterns to watch
        logger: Logger for tracking events and errors
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

        Args:
            event: File system event containing the path of the created file
        """
        if event:
            try:
                with ManagedConnection(self.db_conn) as conn:
                    self.processor.process_file(event.src_path, conn)
            except Exception as e:
                self.logger.error(f"Error processing file {event.src_path}: {e}")
