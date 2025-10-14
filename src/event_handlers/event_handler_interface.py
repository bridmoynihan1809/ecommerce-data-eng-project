from abc import ABC, abstractmethod


class IEventHandler(ABC):
    """
    An interface for event handler classes.

    This class provides an abstract `on_created` method responsible for
    handling events triggered by file changes for raw files
    """
    @abstractmethod
    def on_created(self, event):
        """
        Handles the processing of new raw files.
        """
        pass
