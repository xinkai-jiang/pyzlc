from __future__ import annotations

from typing import Optional

from .loop_manager import LanComLoopManager
from .multicast import MulticastWorker
from .nodes_info_manager import NodesInfoManager
from .zmq_socket_manager import ZMQSocketManager
from ..sockets.service_manager import ServiceManager
from ..sockets.subscriber_manager import SubscriberManager
from ..utils.msg import Empty
from ..utils.log import _logger
from ..utils.node_info import NodeInfo


class LanComNode:
    """Represents a LanCom node in the network."""

    instance: Optional[LanComNode] = None

    @classmethod
    def get_instance(cls) -> LanComNode:
        """Get the singleton instance of LanComNode."""
        if cls.instance is None:
            raise ValueError("LanComNode is not initialized yet.")
        return cls.instance

    @classmethod
    def init(
        cls,
        node_name: str,
        node_ip: str,
        group: str,
        group_port: int,
        group_name: str,
    ) -> None:
        if cls.instance is None:
            cls.instance = LanComNode(node_name, node_ip, group, group_port, group_name)

    def __init__(
        self,
        node_name: str,
        node_ip: str,
        group: str,
        group_port: int,
        group_name: str,
    ) -> None:
        LanComNode.instance = self
        self.name = node_name
        self.node_ip = node_ip
        self.group = group
        self.group_port = group_port
        self.group_name = group_name
        self.zmq_socket_manager: ZMQSocketManager = ZMQSocketManager()
        self.nodes_manager: NodesInfoManager = NodesInfoManager(node_name, node_ip)
        self.loop_manager: LanComLoopManager = LanComLoopManager()
        self.service_manager = ServiceManager(f"tcp://{self.node_ip}:0")
        self.subscriber_manager = SubscriberManager()
        self.multicast_worker = MulticastWorker(
            local_info=self.nodes_manager.local_node_info,
            service_port=self.service_manager.port,
            group=self.group,
            group_port=self.group_port,
            group_name=self.group_name,
        )

    def start_node(self):
        """Start the node's operations."""
        _logger.info("Starting LanCom node...")
        self.running: bool = True
        # add tasks to the event loop
        self.multicast_future = self.loop_manager.submit_loop_task(
            self.multicast_worker.start()
        )
        self.heartbeat_future = self.loop_manager.submit_loop_task(
            self.nodes_manager.check_heartbeat()
        )

    def _get_node_info_handler(self, request: Empty) -> NodeInfo:
        return self.nodes_manager.local_node_info

    def stop_node(self):
        """Stop the node's operations."""
        _logger.info("Stopping LanCom node...")
        self.running = False
        self.service_manager.stop()
        self.multicast_future.cancel()
        self.heartbeat_future.cancel()
        self.loop_manager.stop()
        _logger.info("LanCom node has been stopped")
