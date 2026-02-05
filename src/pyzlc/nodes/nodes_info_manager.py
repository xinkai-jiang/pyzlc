from __future__ import annotations

import asyncio
from typing import Dict, List, Optional, Any, cast
import time

from ..utils.node_info import (
    NodeInfo,
    SocketInfo,
    TopicName,
    HashIdentifier,
)
from .loop_manager import LanComLoopManager
from ..utils.msg import create_hash_identifier, HeartbeatMessage
from ..utils.log import _logger
from ..utils.msg import send_request


class NodesInfoManager:
    """Manages information about nodes in the network."""

    _instance: Optional[NodesInfoManager] = None

    @classmethod
    def get_instance(cls) -> NodesInfoManager:
        """Get the singleton instance of NodesInfoManager."""
        if cls._instance is None:
            raise ValueError("NodesInfoManager is not initialized yet.")
        return cls._instance

    def __init__(self, local_name: str, local_ip: str) -> None:
        NodesInfoManager._instance = self
        self.loop_manager = LanComLoopManager.get_instance()
        self.running = True
        self.nodes_info: Dict[HashIdentifier, NodeInfo] = {}  # keyed by full nodeID
        self.nodes_info_id: Dict[HashIdentifier, int] = {}  # keyed by full nodeID
        self.nodes_heartbeat: Dict[HashIdentifier, float] = {}  # keyed by full nodeID
        # Map int32 node_hash to full nodeID for lookup
        self._hash_to_node_id: Dict[int, HashIdentifier] = {}
        # self.local_name = local_name
        # self.local_ip = local_ip
        # self.local_node_id = create_hash_identifier()
        # self.local_info_id: int = 0
        self.local_node_info: NodeInfo = NodeInfo(
            {
                "name": local_name,
                "nodeID": create_hash_identifier(),
                "infoID": 0,
                "ip": local_ip,
                "topics": [],
                "services": [],
            }
        )

    def check_local_service(self, service_name: str) -> bool:
        """Check if a service is registered locally."""
        for service in self.local_node_info.get("services", []):
            if service["name"] == service_name:
                return True
        return False

    def check_local_topic(self, topic_name: str) -> bool:
        """Check if a topic is registered locally."""
        for topic in self.local_node_info.get("topics", []):
            if topic["name"] == topic_name:
                return True
        return False

    def register_local_service(self, service_name: str, port: int) -> None:
        """Register a new service locally."""
        if self.check_local_service(service_name):
            raise ValueError(f"Service {service_name} is already registered locally.")
        if self.get_service_info(service_name):
            raise ValueError(
                f"Service {service_name} is already registered in the network."
            )
        self.local_node_info["services"].append(
            {"name": service_name, "ip": self.local_node_info.get("ip"), "port": port}
        )
        self.local_node_info["infoID"] += 1

    def register_local_publisher(self, topic_name: str, port: int) -> None:
        """Register a new topic locally."""
        self.local_node_info["topics"].append(
            {"name": topic_name, "ip": self.local_node_info.get("ip"), "port": port}
        )
        self.local_node_info["infoID"] += 1

    def check_node_by_name(self, node_name: str) -> Optional[NodeInfo]:
        """Check if a node with the given name exists."""
        for node_info in self.nodes_info.values():
            if node_info["name"] == node_name:
                return node_info
        return None

    def check_node(self, node_id: HashIdentifier) -> bool:
        """Check if a node with the given ID exists."""
        return node_id in self.nodes_info

    def check_info(self, node_id: HashIdentifier, info_id: int) -> bool:
        """Check if the info ID for a given node matches the provided info ID."""
        return self.nodes_info_id.get(node_id, -1) == info_id

    def update_node(self, node_info: NodeInfo):
        """Update or add a node's information.

        Note: This method is kept for backwards compatibility.
        New code should use handle_heartbeat() instead.
        """
        if not self.check_node(node_info["nodeID"]):
            _logger.info("Node %s has been updated", node_info["name"])
        node_id = node_info["nodeID"]
        if not self.check_info(node_info["nodeID"], node_info["infoID"]):
            self.nodes_info[node_id] = node_info
            self.nodes_info_id[node_id] = node_info["infoID"]
        # in case the node bind to all the interface
        for service in node_info["services"]:
            service["ip"] = node_info["ip"]
        for topic in node_info["topics"]:
            topic["ip"] = node_info["ip"]
        self.nodes_heartbeat[node_id] = time.monotonic()

    def remove_node(self, node_id: HashIdentifier) -> None:
        """Remove a node's information."""
        if self.check_node(node_id):
            _logger.info("Node %s has been removed", self.nodes_info[node_id]["name"])
        self.nodes_info.pop(node_id)
        self.nodes_info_id.pop(node_id)
        self.nodes_heartbeat.pop(node_id)

    def get_publisher_info(self, topic_name: TopicName) -> List[SocketInfo]:
        """Get a list of publisher socket information for a given topic name."""
        publishers: List[SocketInfo] = []
        for pub_info in self.nodes_info.values():
            for pub in pub_info["topics"]:
                if pub["name"] == topic_name:
                    publishers.append(pub)
        for pub in self.local_node_info["topics"]:
            if pub["name"] == topic_name:
                publishers.append(pub)
        return publishers

    def get_service_info(self, service_name: str) -> Optional[SocketInfo]:
        """Get the service socket information for a given service name."""
        for node_info in self.nodes_info.values():
            for service in node_info["services"]:
                if service["name"] == service_name:
                    return service
        for service in self.local_node_info["services"]:
            if service["name"] == service_name:
                return service
        return None

    async def handle_heartbeat_async(
        self, heartbeat_message: HeartbeatMessage, node_ip: str
    ) -> None:
        """Handle a heartbeat message asynchronously.

        Args:
            heartbeat_message: The decoded heartbeat message
            node_ip: IP address of the remote node
        """
        self.nodes_heartbeat[heartbeat_message.node_id] = time.monotonic()
        if self.check_info(heartbeat_message.node_id, heartbeat_message.info_id):
            return
        _logger.info(f"Fetching node info from {node_ip}:{heartbeat_message.service_port}")
        result = await send_request(
            addr=f"tcp://{node_ip}:{heartbeat_message.service_port}",
            service_name="get_node_info",
            request=None,
            timeout=0.3,
        )
        if result is not None:
            node_info = cast(NodeInfo, result)
            node_info["ip"] = node_ip
            self.update_node(node_info)

    async def check_heartbeat(self, interval: float = 1.0) -> None:
        """Periodically check the heartbeat of nodes."""
        try:
            while self.running:
                removed_nodes = []
                for node_id, last_heartbeat in self.nodes_heartbeat.items():
                    if time.monotonic() - last_heartbeat > 3:
                        _logger.warning(
                            "Node %s is considered offline",
                            self.nodes_info[node_id]["name"],
                        )
                        removed_nodes.append(node_id)
                for node_id in removed_nodes:
                    self.remove_node(node_id)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
