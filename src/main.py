from logging import Logger, basicConfig, INFO, FileHandler, StreamHandler, getLogger
from typing import Iterator, List
from dotenv import load_dotenv
from daemons.daemon_factory import DaemonFactory
from processors.processor_factory import ProcessorFactory
from db.postgres_manager import PostgresManager
from db.postgres_db import PostgresDB
from sqlalchemy import create_engine
import os

from src.daemons.daemon_interface import IDaemon
from src.processors.processor_interface import IProcessor

load_dotenv()


def setup_logging(name: str):
    basicConfig(
        level=INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[
            FileHandler("app.log"),
            StreamHandler()
        ]
    )
    return getLogger(name)


def configure_database(logger: Logger):
    pg_client = PostgresDB(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        min_conn=1,
        max_conn=5,
        logger=logger
    )
    engine = create_engine(
        "postgresql+psycopg2://",
        creator=pg_client.get_connection
    )
    pg_manager = PostgresManager(engine=engine, logger=logger)
    return pg_client, pg_manager


def get_processors_and_daemons(processor_types: List[str],
                               pg_manager: PostgresManager,
                               app_logger: Logger) -> Iterator[tuple[str, IProcessor, IDaemon]]:
    processor_factory = ProcessorFactory(pg_manager, app_logger)
    # TODO make daemon params part of configuration unique to each domain
    daemon_factory = DaemonFactory('../../src/landing/orders', True, app_logger)
    for p_type in processor_types:
        processor = processor_factory.get_processor(p_type)
        daemon = daemon_factory.get_daemon(p_type)
        yield p_type, processor, daemon


def main():
    processor_logger = setup_logging('db')
    # app_logger = setup_logging('app')
    pg_client, pg_manager = configure_database(processor_logger)

    # processor_types = ["order", "customer", "product"]

    processor_types = ["order"]

    for p_type, processor, daemon in get_processors_and_daemons(processor_types,
                                                                pg_manager,
                                                                processor_logger):
        processor_logger.info(f"Starting daemon for {p_type}")
        daemon.run(processor, pg_client)


if __name__ == '__main__':
    main()
