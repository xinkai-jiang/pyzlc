from __future__ import annotations

import asyncio
from typing import Dict, List, Optional
import time

from ..utils.node_info import (
    NodeInfo,
    SocketInfo,
    TopicName,
    HashIdentifier,
)
from ..utils.msg import create_hash_identifier
from ..utils.log import _logger
from ..utils.node_info import encode_node_info


class NodesInfoManager:
    """Manages information about nodes in the network."""

    _instance: Optional[NodesInfoManager] = None

    @classmethod
    def get_instance(cls) -> NodesInfoManager:
        """Get the singleton instance of NodesInfoManager."""
        if cls._instance is None:
            raise ValueError("NodesInfoManager is not initialized yet.")
        return cls._instance

    def __init__(self):
        NodesInfoManager._instance = self
        self.running = True
        self.nodes_info: Dict[HashIdentifier, NodeInfo] = {}
        self.nodes_info_id: Dict[HashIdentifier, int] = {}
        self.nodes_heartbeat: Dict[HashIdentifier, float] = {}

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
        return self.nodes_info_id.get(node_id, "") == info_id

    def update_node(self, node_info: NodeInfo):
        """Update or add a node's information."""
        if not self.check_node(node_info["nodeID"]):
            _logger.info("Node %s has been updated", node_info["name"])
        node_id = node_info["nodeID"]
        if not self.check_info(node_info["nodeID"], node_info["infoID"]):
            self.nodes_info[node_id] = node_info
            self.nodes_info_id[node_id] = node_info["infoID"]
        self.nodes_heartbeat[node_id] = time.monotonic()

    def remove_node(self, node_id: HashIdentifier) -> None:
        """Remove a node's information."""
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
        return publishers

    def get_service_info(self, service_name: str) -> Optional[SocketInfo]:
        """Get the service socket information for a given service name."""
        for node_info in self.nodes_info.values():
            for service in node_info["services"]:
                if service["name"] == service_name:
                    return service
        return None

    async def check_heartbeat(self) -> None:
        """Periodically check the heartbeat of nodes."""
        while self.running:
            for node_id, last_heartbeat in self.nodes_heartbeat.items():
                if time.monotonic() - last_heartbeat > 3:
                    _logger.warning(
                        "Node %s is considered offline",
                        self.nodes_info[node_id]["name"],
                    )
                    self.remove_node(node_id)
            await asyncio.sleep(1)


class LocalNodeInfo:
    """Holds local node information."""

    _instance: Optional[LocalNodeInfo] = None

    @classmethod
    def get_instance(cls) -> LocalNodeInfo:
        """Get the singleton instance of LocalNodeInfo."""
        if cls._instance is None:
            raise ValueError("LocalNodeInfo is not initialized yet.")
        return cls._instance

    def __init__(self, name: str, ip: str) -> None:
        LocalNodeInfo._instance = self
        self.nodes_manager: NodesInfoManager = NodesInfoManager.get_instance()
        self.name = name
        self.node_id = create_hash_identifier()
        self.info_id: int = 0
        self.node_info: NodeInfo = NodeInfo(
            {
                "name": self.name,
                "nodeID": self.node_id,
                "infoID": self.info_id,
                "ip": ip,
                "topics": [],
                "services": [],
            }
        )

    def create_heartbeat_message(self) -> bytes:
        """Create a heartbeat message in bytes."""
        return encode_node_info(self.node_info)

    def check_local_service(self, service_name: str) -> bool:
        """Check if a service is registered locally."""
        for service in self.node_info.get("services", []):
            if service["name"] == service_name:
                return True
        return False

    def check_local_topic(self, topic_name: str) -> bool:
        """Check if a topic is registered locally."""
        for topic in self.node_info.get("topics", []):
            if topic["name"] == topic_name:
                return True
        return False

    def register_service(self, service_name: str, port: int) -> None:
        """Register a new service locally."""
        if self.check_local_service(service_name):
            raise ValueError(f"Service {service_name} is already registered locally.")
        if self.nodes_manager.get_service_info(service_name):
            raise ValueError(
                f"Service {service_name} is already registered in the network."
            )
        self.node_info.setdefault("services", []).append(
            {"name": service_name, "ip": self.node_info.get("ip"), "port": port}
        )
        self.info_id += 1

    def register_publisher(self, topic_name: str, port: int) -> None:
        """Register a new topic locally."""
        if self.check_local_topic(topic_name):
            raise ValueError(f"Topic {topic_name} is already registered locally.")
        if len(self.nodes_manager.get_publisher_info(topic_name)) > 0:
            raise ValueError(
                f"Topic {topic_name} is already registered in the network."
            )
        self.node_info.setdefault("topics", []).append(
            {"name": topic_name, "ip": self.node_info.get("ip"), "port": port}
        )
        self.info_id += 1
