from __future__ import annotations

from typing import Dict, List, Optional
import traceback
import msgpack

from ..utils.node_info import (
    NodeInfo,
    SocketInfo,
    TopicName,
    HashIdentifier,
)

from ..utils.log import logger
from ..utils.msg import send_bytes_request

class NodesInfoManager:
    """Manages information about nodes in the network."""
    def __init__(self):
        self.nodes_info: Dict[HashIdentifier, NodeInfo] = {}
        self.nodes_info_id: Dict[HashIdentifier, int] = {}
        self.nodes_heartbeat: Dict[HashIdentifier, str] = {}

    def check_node(self, node_id: HashIdentifier) -> bool:
        """Check if a node with the given ID exists."""
        return node_id in self.nodes_info

    def check_info(self, node_id: HashIdentifier, info_id: int) -> bool:
        """Check if the info ID for a given node matches the provided info ID."""
        return self.nodes_info_id.get(node_id, "") == info_id

    def check_multicast_message(self, node_id: HashIdentifier, info_id: int) -> bool:
        """Check if the heartbeat for a given node is valid."""
        return self.check_node(node_id) and self.check_info(node_id, info_id)

    def update_node(self, node_id: HashIdentifier, node_info: NodeInfo):
        """Update or add a node's information."""
        if node_id in self.nodes_info:
            logger.info("Node %s has been updated", node_info["name"])
        else:
            logger.info("Node %s has been added", node_info["name"])
        self.nodes_info[node_id] = node_info
        self.nodes_info_id[node_id] = node_info["infoID"]
        self.nodes_heartbeat[node_id] = node_info["ip"]

    def remove_node(self, node_id: HashIdentifier) -> None:
        """Remove a node's information."""
        self.nodes_info.pop(node_id, None)
        self.nodes_info_id.pop(node_id, None)
        self.nodes_heartbeat.pop(node_id, None)

    def get_publisher_info(self, topic_name: TopicName) -> List[SocketInfo]:
        """Get a list of publisher socket information for a given topic name."""
        publishers: List[SocketInfo] = []
        for pub_info in self.nodes_info.values():
            for pub in pub_info.get("pubList", []):
                if pub["name"] == topic_name:
                    publishers.append(pub)
        return publishers

    def get_service_info(self, service_name: str) -> Optional[SocketInfo]:
        """Get the service socket information for a given service name."""
        for node_info in self.nodes_info.values():
            for service in node_info.get("srvList", []):
                if service["name"] == service_name:
                    return service
        return None

    async def process_new_discover(self, node_ip: str, port: int) -> None:
        """Process a heartbeat message from a node."""
        try:
            addr = f"tcp://{node_ip}:{port}"
            node_info_bytes = await send_bytes_request(addr, "GetNodeInfo", b"")
            if node_info_bytes is not None:
                node_info = msgpack.unpackb(node_info_bytes, strict_map_key=False, raw=False)
                node_id = node_info["nodeID"]
                node_info["ip"] = node_ip
                self.update_node(node_id, node_info)
        except Exception as e:
            traceback.print_exc()
            logger.error("Error processing new discovery: %s", e)