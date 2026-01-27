from __future__ import annotations

import abc
import asyncio
import concurrent.futures
import time
import traceback
import threading
from asyncio import AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine, Optional, Callable, TypeVar

from ..utils.log import _logger

TaskReturnT = TypeVar("TaskReturnT")


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
        self._stopped_event = threading.Event()
        while self._loop is None:
            time.sleep(0.01)

    def spin_task(self) -> None:
        """Start the event loop and run it forever."""
        _logger.info("Starting spin task")
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._running = True
            self._loop.run_forever()
        except Exception as e:
            _logger.error("Unexpected error in thread_task: %s", e)
            traceback.print_exc()
            raise e
        finally:
            _logger.info("Shutting down spin task")
            self._running = False
            if self._loop is not None:
                self._loop.close()
            self._stopped_event.set()
            _logger.info("Spin task has been stopped")

    def spin(self) -> None:
        """Start the spin task in a separate thread."""
        try:
            self._stopped_event.wait()
        except KeyboardInterrupt:
            self.stop()
            raise KeyboardInterrupt

    def stop(self):
        """Stop the event loop and shut down the thread pool executor."""
        self._running = False
        # When loop out of run_forver(), all the tasks are pending
        # need to cancel them
        for task in asyncio.all_tasks(self._loop):
            task.cancel()
        try:
            if self._loop is None:
                raise RuntimeError("Event loop is not initialized")
            self._loop.call_soon_threadsafe(self._loop.stop)
            _logger.info("Event loop stop signal sent")
        except RuntimeError as e:
            _logger.error("One error occurred when stop loop manager: %s", e)
            traceback.print_exc()
        self._stopped_event.wait()
        assert self._executor is not None
        self._executor.shutdown(wait=True)
        _logger.info("Thread pool executor has been shut down")
        _logger.info("LanComLoopManager has been stopped")

    async def run_in_executor(
        self, func: Callable[..., TaskReturnT], *args: Any
    ) -> TaskReturnT:
        """
        Run a synchronous function in the executor.

        Args:
            func: The callable to run.
            *args: Positional arguments for the function.

        Returns:
            The result of the function (can be a single value or a tuple).
        """
        if self._loop is None:
            raise RuntimeError("Event loop is not initialized")

        # In Python 3.9, positional args are natively supported
        # and highly efficient in run_in_executor.
        return await self._loop.run_in_executor(self._executor, func, *args)

    def submit_loop_task(
        self,
        task: Coroutine[Any, Any, TaskReturnT],
    ) -> concurrent.futures.Future:
        """Submit a coroutine task to the event loop.

        Args:
            task (Coroutine[Any, Any, TaskReturnT]): The coroutine task to be submitted.
            block (bool, optional): Whether to block until the task is complete. Defaults to False.
        Raises:
            RuntimeError: If the event loop is not running.

        Returns:
            Union[concurrent.futures.Future, TaskReturnT]: The future representing the execution of the coroutine.
        """
        if not self._loop:
            raise RuntimeError("The event loop is not running")

        async def wrapper():
            try:
                result = await task
            except Exception as e:
                _logger.error("Error in submitted task: %s %s", task.__name__, e)
                traceback.print_exc()
                raise e
            return result

        return asyncio.run_coroutine_threadsafe(wrapper(), self._loop)

    def submit_loop_task_and_wait(
        self, task: Coroutine[Any, Any, TaskReturnT]
    ) -> TaskReturnT:
        """Submit a coroutine task to the event loop.

        Args:
            task (Coroutine[Any, Any, TaskReturnT]): The coroutine task to be submitted.
            block (bool, optional): Whether to block until the task is complete. Defaults to False.
        Raises:
            RuntimeError: If the event loop is not running.

        Returns:
            Union[concurrent.futures.Future, TaskReturnT]: The future representing the execution of the coroutine.
        """
        if not self._loop:
            raise RuntimeError("The event loop is not running")
        future = asyncio.run_coroutine_threadsafe(task, self._loop)
        return future.result()

    def submit_thread_pool_task(
        self, func: Callable[..., TaskReturnT], *args: Any
    ) -> concurrent.futures.Future:
        """Submit a synchronous function to the thread pool executor.

        Args:
            func: The callable to run.
            *args: Positional arguments for the function.

        Returns:
            concurrent.futures.Future: A future representing the execution of the function.
        """
        if self._executor is None:
            raise RuntimeError("Thread pool executor is not initialized")
        return self._executor.submit(func, *args)