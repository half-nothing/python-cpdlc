from threading import Event, Lock, Thread
from typing import Callable, Optional


class Poller:
    _poll_function: Callable[[], None]
    _min_interval: int
    _max_interval: int
    _lock: Lock
    _exit_event: Event
    _task: Optional[Thread]

    def __init__(self, poll_function: Callable[[], None], min_interval: int = 15, max_interval: int = 30) -> None:
        """
        Constructor for Poller class
        Args:
            poll_function (Callable[[], None]): Function to be executed at regular intervals
            min_interval (int): Minimum interval to poll
            max_interval (int): Maximum interval to poll
        """
        ...

    def _polling_loop(self) -> None:
        """
        Internal loop execution function
        """
        ...

    def set_interval(self, min_interval: int, max_interval: int) -> None:
        """
        Change execute interval
        Args:
            min_interval (int): Minimum interval to poll
            max_interval (int): Maximum interval to poll
        Raises:
            ValueError: When min_interval greater than max_interval
        """
        ...

    def start(self) -> None:
        """
        Start polling thread
        """
        ...

    def stop(self) -> None:
        """
        Stop polling thread
        """
        ...
