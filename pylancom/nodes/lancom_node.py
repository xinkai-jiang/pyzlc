from __future__ import annotations

import asyncio
import traceback
from typing import Optional, Callable, Any
import socket
import struct
import time
import zmq

from ..utils.node_info import IPAddress
from ..utils.log import logger
from ..utils.msg import create_hash_identifier
from .loop_manager import LanComLoopManager
from ..utils.node_info import NodeInfo
from .nodes_info_manager import NodesInfoManager
from .zmq_socket_manager import ZMQSocketManager, PUB
from .service_manager import ServiceManager
from ..utils.msg import send_bytes_request


HEARTBEAT_FORMAT = struct.Struct("!6s36sII")


class LocalNodeInfo:
    """Holds local node information."""

    def __init__(self, name: str, ip: IPAddress, port: int) -> None:
        self.name = name
        self.node_id = create_hash_identifier()
        self.info_id: int = 0
        self.port = port
        self.node_info: NodeInfo = NodeInfo({
            "name": self.name,
            "nodeID": self.node_id,
            "infoID": self.info_id,
            "ip": ip,
            "port": self.port,
            "topics": [],
            "services": [],
        })

    def create_heartbeat_message(self) -> bytes:
        """Create a heartbeat message in bytes."""
        return HEARTBEAT_FORMAT.pack(
            b"LANCOM",
            self.node_id.encode(),
            self.info_id,
            self.port,
        )

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

    def register_service(self, service_name: str) -> bool:
        """Register a new service locally."""
        if self.check_local_service(service_name):
            return False
        self.node_info.setdefault("services", []).append(
            {"name": service_name}
        )
        self.info_id += 1
        return True

    def register_publisher(
        self,
        topic_name: str,
    ) -> None:
        pass

# NOTE: asyncio.loop.sock_recvfrom can only be used after Python 3.11
# So we create a custom DatagramProtocol for multicast discovery
class MulticastDiscoveryProtocol(asyncio.DatagramProtocol):
    """DatagramProtocol for handling multicast discovery messages"""

    def __init__(self, node_manager: LanComNode):
        self.loop_manager = node_manager.loop_manager
        self.node_info_manager = node_manager.nodes_manager
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.message_size = HEARTBEAT_FORMAT.size

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.transport = transport
        logger.info("Multicast discovery connection established")

    def datagram_received(self, data: bytes, addr: tuple[str, int]):
        """Handle incoming multicast discovery messages"""
        try:
            node_ip = addr[0]
            if len(data) != self.message_size:
                return
            header, node_id, info_id, port = HEARTBEAT_FORMAT.unpack(data)
            node_id = node_id.decode()
            info_id = int(info_id)
            port = int(port)
            if header != b"LANCOM":
                return
            if not self.node_info_manager.check_multicast_message(node_id, info_id):
                self.loop_manager.submit_loop_task(
                    self.node_info_manager.process_new_discover(node_ip, port)
                )
        except Exception as e:
            logger.error("Error processing datagram: %s", e)
            traceback.print_exc()

    def error_received(self, exc):
        logger.error("Multicast protocol error: %s", exc)
    def connection_lost(self, exc):
        if exc:
            logger.error("Multicast connection lost: %s", exc)
        else:
            logger.error("Multicast discovery connection closed")



class LanComNode:
    """Represents a LanCom node in the network."""
    instance: Optional[LanComNode] = None

    def __init__(
        self,
        node_name: str,
        node_ip: IPAddress,
        group: IPAddress = "224.0.0.1",
        group_port: int = 7720,
    ) -> None:
        super().__init__()
        self.node_ip = node_ip
        self.group = group
        self.group_port = group_port
        self.zmq_socket_manager: ZMQSocketManager = ZMQSocketManager()
        self.nodes_manager: NodesInfoManager = NodesInfoManager()
        self.loop_manager: LanComLoopManager = LanComLoopManager()
        self.discovery_transport: Optional[asyncio.DatagramTransport] = None
        self.service_manager = ServiceManager(f"tcp://{self.node_ip}:0")
        self.service_manager.register_service("GetNodeInfo", self.get_node_info)
        self._local_info = LocalNodeInfo(node_name, self.node_ip, self.service_manager.port)
        self._running: bool = True
        # add tasks to the event loop
        self.loop_manager.submit_loop_task(self.multicast_loop())
        self.loop_manager.submit_loop_task(self.discovery_loop())

    def spin(self) -> None:
        """Start the node's event loop."""
        try:
            self._running = True
            self.loop_manager.spin()
        except KeyboardInterrupt:
            self.stop_node()
        finally:
            logger.info("LanCom node spin has been stopped")


    async def multicast_loop(self, interval=1.0):
        """Send multicast heartbeat messages at regular intervals."""
        self._running = True

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.setsockopt(socket.IPPROTO_IP,
                        socket.IP_MULTICAST_IF,
                        socket.inet_aton(self.node_ip))
        while self._running:
            try:
                msg = self._local_info.create_heartbeat_message()
                sock.sendto(msg, (self.group, self.group_port))
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error("Error in multicast loop: %s", e)
                traceback.print_exc()
                break
        sock.close()

    async def discovery_loop(self):
        """Listen for multicast discovery messages and register nodes."""
        # Create multicast socket
        sock = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )
        # Allow reuse of address
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Bind to the port
        sock.bind(("", self.group_port))
        mreq = struct.pack(
            "4s4s",
            socket.inet_aton(self.group),
            socket.inet_aton(self.node_ip),
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        logger.info("Listening for multicast on %s:%s", self.group, self.group_port)
        # Get event loop and create datagram endpoint
        loop = asyncio.get_event_loop()
        try:
            # Create the datagram endpoint with the protocol
            self.discovery_transport, _ = await loop.create_datagram_endpoint(
                lambda: MulticastDiscoveryProtocol(self), sock=sock
            )
            # Keep the loop running until stopped
            while self._running:
                await asyncio.sleep(1)  # Keep the coroutine alive
        except KeyboardInterrupt:
            logger.info("Discovery loop interrupted by user")
        except asyncio.CancelledError:
            logger.info("Discovery loop cancelled...")
        except Exception as e:
            logger.error("Error in discovery loop: %s", e)
            traceback.print_exc()
        finally:
            # Clean up
            if self.discovery_transport:
                self.discovery_transport.close()
            sock.close()
            logger.info("Multicast discovery loop stopped")


    def stop_node(self):
        """Stop the node's operations."""
        self._running = False
        self.loop_manager.stop_node()
        logger.info("LanCom node has been stopped")

    def create_service(
        self,
        service_name: str,
        callback: Callable[[Any], Any],
    ) -> None:
        """Create and register a service with the given name and callback."""
        self._local_info.register_service(service_name)
        self.service_manager.register_service(service_name, callback)

    def wait_for_service(self) -> None:
        """Start the service manager's service loop."""
        self.loop_manager.submit_loop_task(
            self.service_manager.service_loop(
                self.service_manager.res_socket,
                self.service_manager.callable_services,
            ),
            False,
        )

    def request(
        self,
        addr: str,
        service_name: str,
        request_data: bytes,
        timeout: float = 1.0,
    ) -> asyncio.Future:
        """Send a request to a service and get the response asynchronously."""
        if not self._running:
            raise RuntimeError("Node is not running. Cannot send request.")
        return asyncio.ensure_future(
            send_bytes_request(
                addr,
                service_name,
                request_data,
                timeout,
            )
        )

    def get_node_info(self) -> NodeInfo:
        """Get the local node's information."""
        return self._local_info.node_info
