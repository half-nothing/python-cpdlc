import asyncio
from random import randint
from typing import Callable, Optional


class AdaptivePoller:
    def __init__(
            self,
            poll_function: Callable[[], None],
            min_normal_interval: int = 15,
            max_normal_interval: int = 30
    ):
        """
        自适应轮询调度器

        :param poll_function: 轮询时执行的回调函数
        :param min_normal_interval: 正常轮询最小间隔（秒）
        :param max_normal_interval: 正常轮询最大间隔（秒）
        """
        self.poll_function = poll_function
        self.min_normal_interval = min_normal_interval
        self.max_normal_interval = max_normal_interval

        self.fast_mode_remaining = 0
        self.is_running = False
        self.task: Optional[asyncio.Task] = None

    async def _polling_loop(self):
        """轮询主循环"""
        while self.is_running:
            interval = randint(self.min_normal_interval, self.max_normal_interval)

            try:
                await asyncio.sleep(interval)
                if self.is_running:
                    self.poll_function()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"轮询异常: {e}")

    async def start(self):
        """启动轮询器（异步方法）"""
        if not self.is_running:
            self.is_running = True
            self.task = asyncio.create_task(self._polling_loop())

    async def stop(self):
        """停止轮询器（异步方法）"""
        if self.is_running and self.task:
            self.is_running = False
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
