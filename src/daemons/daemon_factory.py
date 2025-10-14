from logging import Logger
from daemons.daemon_interface import IDaemon
from daemons.order_daemon import OrderDaemon


class DaemonFactory:
    def __init__(self, watch_directory: str, is_daemon: bool, logger: Logger):
        self.watch_directory = watch_directory
        self.is_daemon = is_daemon
        self.logger = logger

    def get_daemon(self, daemon_type: str) -> IDaemon:
        daemon_type = daemon_type.lower()

        if daemon_type == "order":
            return OrderDaemon(self.watch_directory, self.is_daemon, self.logger)
        # elif daemon_type == "customer":
        #     return CustomerDaemon(self.watch_directory, self.is_daemon, self.logger)
        # elif daemon_type == "product":
        #     return ProductDaemon(self.watch_directory, self.is_daemon, self.logger)
        else:
            raise ValueError(f"Unknown daemon type: {daemon_type}")
