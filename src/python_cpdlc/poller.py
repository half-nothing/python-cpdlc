from datetime import datetime
from random import randint
from threading import Event, Lock, Thread
from time import monotonic
from typing import Callable, Optional

from loguru import logger


class Poller:
    """
    Used to execute a function at regular intervals, the time delay fluctuates between min_interval and max_interval

    Attributes:
        _poll_function (Callable[[], None]): Function to be executed at regular intervals
        _min_interval (int): Minimum interval to poll
        _max_interval (int): Maximum interval to poll
        _lock (threading.Lock): Lock to acquire lock
        _exit_event (threading.Event): Thread exit event
        _task (threading.Thread): Thread handler
    """

    def __init__(
            self,
            poll_function: Callable[[], None],
            min_interval: int = 15,
            max_interval: int = 30
    ):
        """
        Constructor for Poller class
        Args:
            poll_function (Callable[[], None]): Function to be executed at regular intervals
            min_interval (int): Minimum interval to poll
            max_interval (int): Maximum interval to poll
        """
        logger.trace(f"Poller initializing with "
                     f"min_interval={min_interval}s, max_interval={max_interval}s")
        self._poll_function = poll_function
        self._min_interval = min_interval
        self._max_interval = max_interval
        self._lock = Lock()
        self._exit_event = Event()
        self._task: Optional[Thread] = None
        logger.trace("Poller initialized")

    def _polling_loop(self) -> None:
        """
        Internal loop execution function
        """
        logger.trace(f"Poll thread started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        while not self._exit_event.is_set():
            with self._lock:
                interval = randint(self._min_interval, self._max_interval)

            try:
                start_time = monotonic()
                self._poll_function()
                elapsed = monotonic() - start_time
                logger.trace(f"Current polling loop elapsed time: {elapsed:.6}s")
            except Exception as e:
                logger.error(f"Exception occurred while polling: {e}")

            self._exit_event.wait(timeout=interval)
        logger.trace(f"Poll thread stopped at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def set_interval(self, min_interval: int, max_interval: int) -> None:
        """
        Change execute interval
        Args:
            min_interval (int): Minimum interval to poll
            max_interval (int): Maximum interval to poll
        Raises:
            ValueError: When min_interval greater than max_interval
        """
        if min_interval > max_interval:
            logger.error(f"Min interval must be less than max interval but got {min_interval} and {max_interval}")
            raise ValueError(f"min_interval={min_interval} > max_interval={max_interval}")
        with self._lock:
            self._min_interval = min_interval
            self._max_interval = max_interval

    def start(self):
        """
        Start polling thread
        """
        if self._task is None or not self._task.is_alive():
            logger.debug(f"Poll thread starting")
            self._exit_event.clear()
            self._task = Thread(target=self._polling_loop, daemon=True)
            self._task.start()

    def stop(self):
        """
        Stop polling thread
        """
        if self._task and self._task.is_alive():
            logger.debug(f"Poll thread stopping")
            self._exit_event.set()
            self._task.join()
            self._task = None
