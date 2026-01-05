# Add these lines at the top of your pyzlc/__init__.py
from __future__ import annotations
import asyncio
import platform
from typing import Callable, Any, Optional
import time
import importlib.metadata

from .nodes.lancom_node import LanComNode
from .nodes.nodes_info_manager import NodeInfo, LocalNodeInfo, NodesInfoManager
from .sockets.service_client import ServiceProxy
from .sockets.publisher import Publisher
from .sockets.service_manager import Empty, empty
from .utils.log import _logger


# Fix for Windows event loop to avoid ZMQ warnings
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore


try:
    __version__ = importlib.metadata.version("pyzlc")
except importlib.metadata.PackageNotFoundError:
    # package is not installed (e.g. running from source)
    __version__ = "unknown"


__all__ = [
    "Publisher",
    "init",
    "sleep",
    "spin",
    "call",
    "register_service_handler",
    "register_subscriber_handler",
    "wait_for_service",
    "check_node_info",
    "empty",
    "Empty",
    "info",
    "debug",
    "warning",
    "error",
    "remote_log",
]



def init(node_name: str, node_ip: str) -> None:
    """Initialize the LanCom node singleton."""
    if LanComNode.instance is not None:
        raise ValueError("Node is already initialized.")
    LanComNode.init(node_name, node_ip)


def sleep(duration: float) -> None:
    """Sleep for the specified duration in seconds."""
    try:
        time.sleep(duration)
    except KeyboardInterrupt:
        _logger.debug("Sleep interrupted by user")
        LanComNode.get_instance().stop_node()


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
) -> Any:
    """Call a service with the specified name and request."""
    ServiceProxy.request(service_name, request)

def register_service_handler(
    service_name: str,
    callback: Callable,
) -> None:
    """Create a service with the specified name and callback."""
    service_manager = LanComNode.get_instance().service_manager
    LocalNodeInfo.get_instance().register_service(service_name, service_manager.port)
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
) -> None:
    """Start the service manager's service loop."""
    waited_time = 0
    node_info_manager = LanComNode.get_instance().nodes_manager
    while not node_info_manager.get_service_info(service_name):
        if waited_time >= timeout:
            raise TimeoutError(
                f"Service {service_name} is online after {timeout} seconds."
            )
        _logger.info(
            "Waiting for service %s to be registered locally...", service_name
        )
        time.sleep(check_interval)
        waited_time += check_interval

def check_node_info(node_name: str) -> Optional[NodeInfo]:
    """Check the node information."""
    return NodesInfoManager.get_instance().check_node_by_name(node_name)


info = _logger.info
debug = _logger.debug
warning = _logger.warning
error = _logger.error
remote_log = _logger.remote_log
