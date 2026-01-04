from __future__ import annotations

import asyncio
import traceback
from typing import Optional
import socket
import struct

from ..utils.log import _logger

from .loop_manager import LanComLoopManager
from .nodes_info_manager import NodesInfoManager, LocalNodeInfo
from .zmq_socket_manager import ZMQSocketManager
from ..sockets.service_manager import ServiceManager
from ..utils.node_info import decode_node_info
from ..sockets.subscriber_manager import SubscriberManager


# NOTE: asyncio.loop.sock_recvfrom can only be used after Python 3.11
# So we create a custom DatagramProtocol for multicast discovery
class MulticastDiscoveryProtocol(asyncio.DatagramProtocol):
    """DatagramProtocol for handling multicast discovery messages"""

    def __init__(self, node_manager: LanComNode):
        self.loop_manager = node_manager.loop_manager
        self.node_info_manager = node_manager.nodes_manager
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.transport = transport
        _logger.info("Multicast discovery connection established")

    def datagram_received(self, data: bytes, addr: tuple[str, int]):
        """Handle incoming multicast discovery messages"""
        try:
            node_ip = addr[0]
            node_info = decode_node_info(data)
            node_info["ip"] = node_ip
            self.node_info_manager.update_node(node_info)
        except Exception as e:
            _logger.error("Error processing datagram: %s", e)
            traceback.print_exc()
            raise e

    def error_received(self, exc):
        _logger.error("Multicast protocol error: %s", exc)

    def connection_lost(self, exc):
        if exc:
            _logger.error("Multicast connection lost: %s", exc)
        else:
            _logger.error("Multicast discovery connection closed")


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
        group: str = "224.0.0.1",
        group_port: int = 7720,
    ) -> None:
        if cls.instance is None:
            cls.instance = LanComNode(node_name, node_ip, group, group_port)

    def __init__(
        self,
        node_name: str,
        node_ip: str,
        group: str,
        group_port: int,
    ) -> None:
        LanComNode.instance = self
        self.name = node_name
        self.node_ip = node_ip
        self.group = group
        self.group_port = group_port
        self.zmq_socket_manager: ZMQSocketManager = ZMQSocketManager()
        self.nodes_manager: NodesInfoManager = NodesInfoManager()
        self.loop_manager: LanComLoopManager = LanComLoopManager()
        self.discovery_transport: Optional[asyncio.DatagramTransport] = None
        self._local_info = LocalNodeInfo(node_name, self.node_ip)
        self.service_manager = ServiceManager(f"tcp://{self.node_ip}:0")
        self.subscriber_manager = SubscriberManager()
        self.running: bool = True
        # add tasks to the event loop
        self.multicast_future = self.loop_manager.submit_loop_task(
            self.multicast_loop()
        )
        self.discovery_future = self.loop_manager.submit_loop_task(
            self.discovery_loop()
        )
        self.heartbeat_future = self.loop_manager.submit_loop_task(
            self.nodes_manager.check_heartbeat()
        )

    async def multicast_loop(self, interval=1.0):
        """Send multicast heartbeat messages at regular intervals."""
        self.running = True

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.setsockopt(
            socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.node_ip)
        )
        while self.running:
            try:
                msg = self._local_info.create_heartbeat_message()
                sock.sendto(msg, (self.group, self.group_port))
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                _logger.info("Multicast loop cancelled...")
                break
            except Exception as e:
                _logger.error("Error in multicast loop: %s", e)
                traceback.print_exc()
                raise e
        sock.close()
        _logger.info("Multicast heartbeat loop stopped")

    async def discovery_loop(self):
        """Listen for multicast discovery messages and register nodes."""
        # Create multicast socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            # Allow reuse of address
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            # Bind to the port
            sock.bind(("", self.group_port))
            mreq = struct.pack(
                "4s4s",
                socket.inet_aton(self.group),
                socket.inet_aton(self.node_ip),
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            _logger.info(
                "Listening for multicast on %s:%s", self.group, self.group_port
            )
            # Get event loop and create datagram endpoint
            loop = asyncio.get_event_loop()
            # Create the datagram endpoint with the protocol
            self.discovery_transport, _ = await loop.create_datagram_endpoint(
                lambda: MulticastDiscoveryProtocol(self), sock=sock
            )
            await asyncio.Future()
        except KeyboardInterrupt:
            _logger.info("Discovery loop interrupted by user")
        except asyncio.CancelledError:
            _logger.info("Discovery loop cancelled...")
        except Exception as e:
            _logger.error("Error in discovery loop: %s", e)
            traceback.print_exc()
            raise e
        try:
            if self.discovery_transport:
                self.discovery_transport.close()
            sock.close()
            _logger.info("Multicast discovery loop stopped")
        except RuntimeWarning as e:
            _logger.error("Error closing discovery socket: %s", e)
        except Exception as e:
            _logger.error("Unexpected error when closing discovery socket: %s", e)
            traceback.print_exc()
            raise e

    def stop_node(self):
        """Stop the node's operations."""
        self.running = False
        self.service_manager.stop()
        self.multicast_future.cancel()
        self.discovery_future.cancel()
        self.heartbeat_future.cancel()
        self.loop_manager.stop()
        _logger.info("LanCom node has been stopped")
