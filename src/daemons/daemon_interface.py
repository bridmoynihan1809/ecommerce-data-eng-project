from abc import ABC, abstractmethod


class IDaemon(ABC):
    """
    An interface for daemon classes.

    This class provides an abstract `run` method responsible for
    starting and running the daemon's tasks.
    """
    @abstractmethod
    def run(self):
        """
        Starts the execution of the daemon.

        Returns:
            None
        """
        pass
