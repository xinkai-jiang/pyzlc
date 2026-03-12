"""
pyzlc: A lightweight Python library for LanCom communication based on ZeroMQ.

This package provides high-level utilities for node initialization,
message spinning, and service registration.
"""

from __future__ import annotations
import asyncio
import atexit
import platform
from typing import Callable, Any, Optional, List, Coroutine, Union
import time
import concurrent.futures

from .nodes.lancom_node import LanComNode
from .nodes.nodes_info_manager import NodeInfo
from .nodes.loop_manager import LanComLoopManager, DaemonThreadPoolExecutor, TaskReturnT
from .sockets.service_client import zlc_request_async, zlc_request
from .sockets.publisher import Publisher, Streamer
from .utils.msg import Empty, empty, _get_zlc_version
from .utils.log import _logger, LogLevel


# Fix for Windows event loop to avoid ZMQ warnings
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore

__version__ = _get_zlc_version()
__author__ = "Xinkai Jiang"
__email__ = "jiangxinkai98@gmail.com"

__all__: List[str] = [
    "Publisher",
    "Streamer",
    "init",
    "shutdown",
    "sleep",
    "spin",
    "call",
    "async_call",
    "register_service_handler",
    "register_subscriber_handler",
    "wait_for_service",
    "wait_for_service_async",
    "check_node_info",
    "get_node",
    "get_nodes_info",
    "is_running",
    "submit_loop_task",
    "submit_thread_pool_task",
    "empty",
    "Empty",
    "LogLevel",
    "info",
    "debug",
    "warning",
    "error",
]


def init(
    node_name: str,
    node_ip: str,
    group_name: str = "zlc_default_group_name",
    group: str = "224.0.0.1",
    group_port: int = 7720,
    log_level: Union[LogLevel, int] = LogLevel.INFO,
    sub_group: bool = False,
) -> None:
    set_log_level(log_level)
    """Initialize the LanCom node singleton."""
    if LanComNode.get(group_name) is not None:
        raise ValueError("Node is already initialized.")
    LanComNode.init(node_name, node_ip, group, group_port, group_name, sub_group)
    register_service_handler(
        "get_node_info",
        LanComNode.get_instance(group_name)._get_node_info_handler,
        group_name
    )
    LanComNode.get_instance(group_name).start_node()

    # Auto-cleanup on program exit
    atexit.register(shutdown)


def set_log_level(level: Union[LogLevel, int]) -> None:
    """Set the logging level for the LanCom logger."""
    _logger.setLevel(int(level))


def shutdown() -> None:
    """Shutdown the LanCom node. Safe to call multiple times."""
    _logger.debug("Shutting down LanCom node...")
    LanComNode.stop_all_nodes()


def get_node(group_name: Optional[str] = None) -> LanComNode:
    """Get the LanCom node singleton."""
    return LanComNode.get_instance(group_name)


def get_nodes_info(group_name: Optional[str] = None) -> List[NodeInfo]:
    """Get the list of known nodes' information."""
    nodes_manager = LanComNode.get_instance(group_name).nodes_info_manager
    return list(nodes_manager.nodes_info.values())


def sleep(duration: float) -> None:
    """Sleep for the specified duration in seconds."""
    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        _logger.debug("Sleep interrupted by user")
        LanComNode.stop_all_nodes()
        raise


def spin(group_name: Optional[str] = None) -> None:
    """Spin the LanCom node to process incoming messages."""
    try:
        LanComNode.get_instance(group_name).loop_manager.spin()
    except KeyboardInterrupt:
        _logger.debug("LanCom node interrupted by user")
        LanComNode.get_instance(group_name).stop_node()


def call(
    service_name: str,
    request: Any,
    timeout: float = 2.0,
    group_name: Optional[str] = None,
) -> Any:
    """Call a service with the specified name and request."""
    return zlc_request(service_name, request, timeout, group_name)


async def async_call(
    service_name: str,
    request: Any,
    timeout: float = 2.0,
    group_name: Optional[str] = None,
) -> Any:
    """Asynchronously call a service with the specified name and request."""
    return await zlc_request_async(service_name, request, timeout, group_name)


def register_service_handler(
    service_name: str,
    callback: Callable,
    group_name: Optional[str] = None,
) -> None:
    """Create a service with the specified name and callback."""
    service_manager = LanComNode.get_instance(group_name).service_manager
    LanComNode.get_instance(group_name).nodes_info_manager.register_local_service(
        service_name, service_manager.port
    )
    service_manager.register_service(service_name, callback)


def register_subscriber_handler(
    topic_name: str,
    callback: Callable,
    group_name: Optional[str] = None,
) -> None:
    """Create a subscriber for the specified topic."""
    subscriber_manager = LanComNode.get_instance(group_name).subscriber_manager
    subscriber_manager.add_subscriber(topic_name, callback)


def wait_for_service(
    service_name: str,
    timeout: float = 5.0,
    check_interval: float = 0.1,
    group_name: Optional[str] = None,
) -> bool:
    """Start the service manager's service loop."""
    waited_time = 0
    nodes_info_manager = LanComNode.get_instance(group_name).nodes_info_manager
    while not nodes_info_manager.get_service_info(service_name):
        if waited_time >= timeout:
            warning(f"Timeout waiting for service {service_name}")
            return False
        time.sleep(check_interval)
        waited_time += check_interval
    return True


def wait_for_service_async(
    service_name: str,
    timeout: float = 5.0,
    check_interval: float = 0.1,
    group_name: Optional[str] = None,
) -> Coroutine[Any, Any, bool]:
    """Asynchronously wait for a service to become available."""
    return LanComNode.get_instance(group_name).loop_manager.run_in_executor(
        wait_for_service,
        service_name,
        timeout,
        check_interval,
        group_name
    )


def check_node_info(node_name: str, group_name: Optional[str] = None) -> Optional[NodeInfo]:
    """Check the node information."""
    return LanComNode.get_instance(group_name).nodes_info_manager.check_node_by_name(node_name)


def is_running(group_name: Optional[str] = None) -> bool:
    """A simple function to indicate the module is loaded successfully."""
    assert LanComNode.get_instance(group_name) is not None, "LanComNode is not initialized."
    return LanComNode.get_instance(group_name).running


def submit_loop_task(
    task: Coroutine[Any, Any, TaskReturnT],
    group_name: Optional[str] = None
) -> concurrent.futures.Future:
    """Submit a coroutine to the event loop."""
    assert LanComLoopManager is not None, "LanComNode is not initialized."
    return LanComNode.get_instance(group_name).loop_manager.submit_loop_task(task)


def submit_thread_pool_task(
    func: Callable[..., TaskReturnT], *args: Any
) -> concurrent.futures.Future:
    """Submit a synchronous function to the thread pool executor."""
    return DaemonThreadPoolExecutor.submit_thread_pool_task(func, *args)


info = _logger.info
debug = _logger.debug
warning = _logger.warning
error = _logger.error
