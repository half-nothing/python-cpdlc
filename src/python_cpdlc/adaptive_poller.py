import asyncio
from random import randint
from typing import Callable, Optional

from loguru import logger


class AdaptivePoller:
    def __init__(
            self,
            poll_function: Callable[[], None],
            min_normal_interval: int = 15,
            max_normal_interval: int = 30
    ):
        self.poll_function = poll_function
        self.min_normal_interval = min_normal_interval
        self.max_normal_interval = max_normal_interval
        self.fast_mode_remaining = 0
        self.is_running = False
        self.task: Optional[asyncio.Task] = None

    async def _polling_loop(self):
        while self.is_running:
            interval = randint(self.min_normal_interval, self.max_normal_interval)
            try:
                if self.is_running:
                    self.poll_function()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error: {e}")

    def start(self):
        if not self.is_running:
            logger.debug(f"Poll thread started")
            self.is_running = True
            self.task = asyncio.run(self._polling_loop())

    def stop(self):
        if self.is_running and self.task:
            logger.debug(f"Poll thread stopped")
            self.is_running = False
            self.task.cancel()
            self.task = None
