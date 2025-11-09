from logging import Logger, basicConfig, INFO, FileHandler, StreamHandler, getLogger
from typing import Iterator
from dotenv import load_dotenv
from processors.processor_factory import ProcessorFactory
from db.postgres_manager import PostgresManager
from db.postgres_db import PostgresDB
from sqlalchemy import create_engine
from config.processor_configuration import CONFIG
from processors.processor_interface import IProcessor
from db.database_manager_interface import IDatabaseManager
from daemons.daemon import Daemon
import os

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


def get_processors_and_daemons(processor_types: dict[dict],
                               pg_manager: IDatabaseManager,
                               app_logger: Logger) -> Iterator[tuple[str, IProcessor, Daemon]]:
    processor_factory = ProcessorFactory(pg_manager, app_logger)
    for process_name, settings in processor_types.items():
        processor = processor_factory.get_processor(process_name)
        daemon = Daemon(settings["watch_directory"], settings["is_daemon"], app_logger)
        yield process_name, processor, daemon


def main():
    app_logger = setup_logging('db')
    pg_client, pg_manager = configure_database(app_logger)

    for process_name, processor, daemon in get_processors_and_daemons(processor_types=CONFIG,
                                                                pg_manager=pg_manager,
                                                                app_logger=app_logger):
        app_logger.info(f"Starting daemon for {process_name}")
        daemon.run(processor, pg_client)


if __name__ == '__main__':
    main()
