from logging import Logger
from daemons.daemon_interface import IDaemon
from watchdog.observers import Observer
from event_handlers.order_event_handler import OrderEventHandler
from processors.order import OrderProcessor
import os
import time

from db.postgres_db import PostgresDB


class OrderDaemon(IDaemon):
    """
    A daemon that watches a directory for new raw order files.

    This class inherits from `IDaemon` and implements the `run` method.
    It processes the file using a `OrderProcessor`, inserting data into a database and handling any necessary
    database interactions.
    """
    def __init__(self, watch_directory: str, is_daemon: bool, logger: Logger) -> None:
        self.observer = Observer()
        self.watch_directory = watch_directory
        self.is_daemon = is_daemon
        self.logger = logger

    def run(self, processor: OrderProcessor, db_conn: PostgresDB) -> None:
        """
        Starts the OrderDaemon to monitor the directory and process files.

        This method sets up the necessary tables, creates a `OrderEventHandler`, and starts the
        `Observer` to watch the specified directory. When a new file is detected,
        it is processed using `OrderProcessor`.
        """
        self.logger.info("Running Order Daemon...")
        processor.set_up_tables()
        order_event_handler = OrderEventHandler(processor, db_conn, ['*.csv'], self.logger)

        self.observer.daemon = self.is_daemon
        watch_path = os.path.abspath(os.path.join(os.path.dirname(__file__), self.watch_directory))
        self.logger.info(f"Watching: {watch_path}")
        self.observer.schedule(event_handler=order_event_handler, path=watch_path, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(600)
        except KeyboardInterrupt:
            self.observer.stop()
        finally:
            self.observer.join()
