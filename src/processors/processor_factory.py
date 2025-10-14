from logging import Logger
from db.database_manager_interface import IDatabaseManager
from processors.order import OrderProcessor
from processors.processor_interface import IProcessor


class ProcessorFactory:
    def __init__(self, database_manager: IDatabaseManager, logger: Logger):
        self.database_manager = database_manager
        self.logger = logger

    def get_processor(self, processor_type: str) -> IProcessor:
        processor_type = processor_type.lower()

        if processor_type == "order":
            return OrderProcessor(self.database_manager, self.logger)
        # elif processor_type == "customer":
        #     return CustomerProcessor(self.database_manager, self.logger)
        # elif processor_type == "product":
        #     return ProductProcessor(self.database_manager, self.logger)
        else:
            raise ValueError(f"Unknown processor type: {processor_type}")
