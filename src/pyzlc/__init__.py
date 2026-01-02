# Add these lines at the top of your pyzerolancom/__init__.py
from __future__ import annotations
import asyncio
import platform

# from pyzerolancom.abstract_node import AbstractNode
from .nodes.lancom_node import LanComNode

from .sockets.publisher import Publisher

# Fix for Windows event loop to avoid ZMQ warnings
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore

def init_node(node_name: str, node_ip: str) -> LanComNode:
    """Initialize the LanCom node singleton."""
    if LanComNode.instance is None:
        return LanComNode(node_name, node_ip)
    raise ValueError("Node is already initialized.")

def get_node() -> LanComNode:
    """Get the existing LanCom node singleton."""
    if LanComNode.instance is None:
        raise ValueError("Node is not initialized yet.")
    return LanComNode.instance
