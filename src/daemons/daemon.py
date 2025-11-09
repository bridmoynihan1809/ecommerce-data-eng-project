from logging import Logger
from watchdog.observers import Observer
from event_handlers.data_event_handler import DataEventHandler
from processors.processor_interface import IProcessor
import os
import time

# from db.postgres_db import PostgresDB
from db.database_interface import IDatabase


class Daemon():
    """
    A daemon that watches a directory for new raw files.

    It processes files using a generic IProcessor, inserting data into a database and handling
    any necessary database interactions.
    """
    def __init__(self, watch_directory: str, is_daemon: bool, logger: Logger) -> None:
        self.observer = Observer()
        self.watch_directory = watch_directory
        self.is_daemon = is_daemon
        self.logger = logger

    def run(self, processor: IProcessor, db_conn: IDatabase) -> None:
        """
        Starts the PostgresDaemon to monitor the directory and process files.

        This method sets up the necessary tables, creates a generic DataEventHandler, and starts the
        Observer to watch the specified directory. When a new file is detected,
        it is processed using the provided IProcessor.

        Args:
            processor: IProcessor instance for processing files (configured for specific entity type)
            db_conn: PostgresDB instance for database connections
        """
        self.logger.info("Running Daemon...")
        processor.set_up_tables()
        event_handler = DataEventHandler(processor, db_conn, ['*.csv'], self.logger)

        self.observer.daemon = self.is_daemon
        watch_path = os.path.abspath(os.path.join(os.path.dirname(__file__), self.watch_directory))
        self.logger.info(f"Watching: {watch_path}")
        self.observer.schedule(event_handler=event_handler, path=watch_path, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(600)
        except KeyboardInterrupt:
            self.observer.stop()
        finally:
            self.observer.join()
