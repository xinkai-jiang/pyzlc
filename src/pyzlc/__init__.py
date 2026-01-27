"""
pyzlc: A lightweight Python library for LanCom communication based on ZeroMQ.

This package provides high-level utilities for node initialization, 
message spinning, and service registration.
"""
from __future__ import annotations
import asyncio
import platform
from typing import Callable, Any, Optional, List, Coroutine
import time
import concurrent.futures

from .nodes.lancom_node import LanComNode
from .nodes.nodes_info_manager import NodeInfo, NodesInfoManager
from .nodes.loop_manager import LanComLoopManager, TaskReturnT
from .sockets.service_client import zlc_request_async, zlc_request
from .sockets.publisher import Publisher, Streamer
from .utils.msg import Empty, empty, _get_zlc_version
from .utils.log import _logger


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
    "empty",
    "Empty",
    "info",
    "debug",
    "warning",
    "error",
    "remote_log",
]


def init(
    node_name: str,
    node_ip: str,
    group: str = "224.0.0.1",
    group_port: int = 7720,
    group_name: str = "zlc_default_group_name",
) -> None:
    """Initialize the LanCom node singleton."""
    if LanComNode.instance is not None:
        raise ValueError("Node is already initialized.")
    LanComNode.init(node_name, node_ip, group, group_port, group_name)
    register_service_handler(
        "get_node_info", LanComNode.get_instance()._get_node_info_handler
    )
    LanComNode.get_instance().start_node()

def shutdown() -> None:
    """Shutdown the LanCom node."""
    node = LanComNode.get_instance()
    node.stop_node()

def get_node() -> LanComNode:
    """Get the LanCom node singleton."""
    return LanComNode.get_instance()


def get_nodes_info() -> List[NodeInfo]:
    """Get the list of known nodes' information."""
    nodes_manager = NodesInfoManager.get_instance()
    return list(nodes_manager.nodes_info.values())


def sleep(duration: float) -> None:
    """Sleep for the specified duration in seconds."""
    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        _logger.debug("Sleep interrupted by user")
        LanComNode.get_instance().stop_node()
        raise


def spin() -> None:
    """Spin the LanCom node to process incoming messages."""
    try:
        LanComNode.get_instance().loop_manager.spin()
    except KeyboardInterrupt:
        _logger.debug("LanCom node interrupted by user")
        LanComNode.get_instance().stop_node()
    finally:
        _logger.info("LanCom node has been stopped")


def call(
    service_name: str,
    request: Any,
    timeout: float = 2.0,
) -> Any:
    """Call a service with the specified name and request."""
    return zlc_request(service_name, request, timeout)


async def async_call(
    service_name: str,
    request: Any,
    timeout: float = 2.0,
) -> Any:
    """Asynchronously call a service with the specified name and request."""
    return await zlc_request_async(service_name, request, timeout)


def register_service_handler(
    service_name: str,
    callback: Callable,
) -> None:
    """Create a service with the specified name and callback."""
    service_manager = LanComNode.get_instance().service_manager
    NodesInfoManager.get_instance().register_local_service(
        service_name, service_manager.port
    )
    service_manager.register_service(service_name, callback)


def register_subscriber_handler(
    topic_name: str,
    callback: Callable,
) -> None:
    """Create a subscriber for the specified topic."""
    subscriber_manager = LanComNode.get_instance().subscriber_manager
    subscriber_manager.add_subscriber(topic_name, callback)


def wait_for_service(
    service_name: str,
    timeout: float = 5.0,
    check_interval: float = 0.1,
) -> bool:
    """Start the service manager's service loop."""
    waited_time = 0
    node_info_manager = LanComNode.get_instance().nodes_manager
    while not node_info_manager.get_service_info(service_name):
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
) -> Coroutine[Any, Any, bool]:
    """Asynchronously wait for a service to become available."""
    return LanComLoopManager.get_instance().run_in_executor(
        wait_for_service,
        service_name,
        timeout,
        check_interval,
    )


def check_node_info(node_name: str) -> Optional[NodeInfo]:
    """Check the node information."""
    return NodesInfoManager.get_instance().check_node_by_name(node_name)


def is_running() -> bool:
    """A simple function to indicate the module is loaded successfully."""
    assert LanComNode.instance is not None, "LanComNode is not initialized."
    return LanComNode.instance.running


def submit_loop_task(
    task: Coroutine[Any, Any, TaskReturnT]
) -> concurrent.futures.Future:
    """Submit a coroutine to the event loop."""
    assert LanComLoopManager is not None, "LanComNode is not initialized."
    return LanComLoopManager.get_instance().submit_loop_task(task)

def submit_thread_pool_task(
    func: Callable[..., TaskReturnT], *args: Any
) -> concurrent.futures.Future:
    """Submit a synchronous function to the thread pool executor."""
    assert LanComLoopManager is not None, "LanComNode is not initialized."
    return LanComLoopManager.get_instance().submit_thread_pool_task(func, *args)

info = _logger.info
debug = _logger.debug
warning = _logger.warning
error = _logger.error
remote_log = _logger.remote_log
