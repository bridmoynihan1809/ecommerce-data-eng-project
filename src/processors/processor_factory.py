from logging import Logger
from db.database_manager_interface import IDatabaseManager
from processors.data_processor import PostgresDataProcessor
from processors.processor_interface import IProcessor
from processors.config_factory import create_customer_config, create_order_config


class ProcessorFactory:
    """
    Factory for creating configured data processors.

    This factory creates generic DataProcessor instances configured for different
    entity types using configuration objects.
    This eliminates the need for separate processor classes for each entity type.

    Args:
        database_manager: IDatabaseManager for executing database operations
        logger: Logger for tracking processing events
    """
    def __init__(self, database_manager: IDatabaseManager, logger: Logger):
        self.database_manager = database_manager
        self.logger = logger

    def get_processor(self, processor_type: str) -> IProcessor:
        """
        Creates a processor instance for the specified entity type.

        Args:
            processor_type: Type of processor to create
        """
        processor_type = processor_type.lower()

        if processor_type == "orders":
            config = create_order_config()
            return PostgresDataProcessor(config, self.database_manager, self.logger)
        elif processor_type == "customers":
            config = create_customer_config()
            return PostgresDataProcessor(config, self.database_manager, self.logger)
        # elif processor_type == "product":
        #     config = create_product_config()
        #     return MySQLDataProcessor(config, self.database_manager, self.logger)
        else:
            raise ValueError(f"Unknown processor type: {processor_type}")
