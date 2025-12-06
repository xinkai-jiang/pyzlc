from __future__ import annotations

import abc
import asyncio
import concurrent.futures
import time
import traceback
from asyncio import AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine, Optional, Union, Callable

from ..utils.log import logger


class LanComLoopManager(abc.ABC):
    """Manages the event loop and thread pool for asynchronous tasks."""

    instance: Optional[LanComLoopManager] = None

    @classmethod
    def get_instance(cls) -> LanComLoopManager:
        """Get the singleton instance of LanComLoopManager."""
        if cls.instance is None:
            cls.instance = cls()
        return cls.instance

    def __init__(self, max_workers: int = 3):
        """Initialize the LanComLoopManager with a thread pool executor.

        Args:
            max_workers (int, optional): The maximum number of worker threads. Defaults to 3.
        """
        LanComLoopManager.instance = self
        self._loop: Optional[AbstractEventLoop] = None
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._executor.submit(self.spin_task)
        self._running: bool = False
        while self._loop is None:
            time.sleep(0.01)

    def spin_task(self) -> None:
        """Start the event loop and run it forever."""
        logger.info("Starting spin task")
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._running = True
            self._loop.run_forever()
        except KeyboardInterrupt:
            self.stop_node()
        except Exception as e:
            logger.error("Unexpected error in thread_task: %s", e)
            traceback.print_exc()
            self.stop_node()
            raise e
        finally:
            if self._loop is not None:
                self._loop.close()
            logger.info("Spin task has been stopped")
            self._running = False
            self._executor.shutdown(wait=False)
            logger.info("Thread pool has been stopped")

    def spin(self) -> None:
        """Start the spin task in a separate thread."""
        while self._running:
            time.sleep(0.05)

    def stop_node(self):
        """Stop the event loop and shut down the thread pool executor."""
        self._running = False
        try:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._loop.stop)
        except RuntimeError as e:
            logger.error("One error occurred when stop server: %s", e)
        assert self._executor is not None
        self._executor.shutdown(wait=False)

    async def run_in_executor(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run a function in the event loop's executor.

        Args:
            task (Coroutine): The coroutine task to be submitted.
            block (bool, optional): Whether to block until the task is complete. Defaults to False.
        Raises:
            RuntimeError: If the event loop is not running.

        Returns:
            Union[concurrent.futures.Future, Any]: The future representing the execution of the coroutine.
        """
        if self._loop is None:
            raise RuntimeError("Event loop is not initialized")
        return await self._loop.run_in_executor(self._executor, func, *args, **kwargs)

    def submit_loop_task(
        self,
        task: Coroutine,
        block: bool = False,
    ) -> Union[concurrent.futures.Future, Any]:
        """Submit a coroutine task to the event loop.

        Args:
            task (Coroutine): The coroutine task to be submitted.
            block (bool, optional): Whether to block until the task is complete. Defaults to False.
        Raises:
            RuntimeError: If the event loop is not running.

        Returns:
            Union[concurrent.futures.Future, Any]: The future representing the execution of the coroutine.
        """
        if not self._loop:
            raise RuntimeError("The event loop is not running")
        future = asyncio.run_coroutine_threadsafe(
            task, self._loop
        )
        if block:
            return future.result()
        return future
