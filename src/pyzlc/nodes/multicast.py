from __future__ import annotations
import asyncio
import socket
import struct
import traceback
from typing import Tuple, Optional

from ..nodes.nodes_info_manager import NodesInfoManager
from ..utils.log import _logger
from ..utils.msg import _parse_version, is_in_same_subnet, HeartbeatMessage, decode_heartbeat_message
from ..utils.event import Event
from ..utils.node_info import NodeInfo





class MulticastWorker:

    def __init__(
        self,
        local_info: NodeInfo,
        service_port: int,
        group: str,
        group_port: int,
        group_name: str
    ) -> None:
        self.local_info = local_info
        self.local_ip = local_info["ip"]
        self.service_port = service_port
        self.group = group
        self.group_port = group_port
        self.group_name = group_name
        self.protocol_version: Tuple[int, int, int] = _parse_version()
        self.node_info_event = Event(HeartbeatMessage)

    async def start(self):
        """Start the multicast worker."""
        self.running = True
        await asyncio.gather(
            self.multicast_loop(),
            self.discovery_loop(),
        )


    async def multicast_loop(self, interval=0.5):
        """Send multicast heartbeat messages at regular intervals."""

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.setsockopt(
            socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.local_ip)
        )
        _service_port = self.service_port
        while self.running:
            try:
                msg = HeartbeatMessage(
                    zlc_version=self.protocol_version,
                    node_id=self.local_info["nodeID"],
                    info_id=self.local_info["infoID"],
                    service_port=_service_port,
                    group_name=self.group_name,
                )
                sock.sendto(msg.to_bytes(), (self.group, self.group_port))
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
                socket.inet_aton(self.local_ip),
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



# NOTE: asyncio.loop.sock_recvfrom can only be used after Python 3.11
# So we create a custom DatagramProtocol for multicast discovery
class MulticastDiscoveryProtocol(asyncio.DatagramProtocol):
    """DatagramProtocol for handling multicast discovery messages"""

    def __init__(self, multicast_worker: MulticastWorker):
        # self.loop_manager = node_manager.loop_manager
        self.local_ip = multicast_worker.local_ip
        self.local_node_id = multicast_worker.local_info["nodeID"]
        self.group_name = multicast_worker.group_name
        self.transport: Optional[asyncio.DatagramTransport] = None
        

    def connection_made(self, transport: asyncio.DatagramTransport):
        self.transport = transport
        _logger.info("Multicast discovery connection established")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        """Handle incoming multicast discovery messages"""
        try:
            node_ip = addr[0]
            if not is_in_same_subnet(self.local_ip, node_ip):
                return
            # Decode the lightweight heartbeat message
            heartbeat_message = decode_heartbeat_message(data)
            # Filter by group name
            if heartbeat_message.group_name != self.group_name:
                return
            # Ignore own heartbeat (compare int32 hashes)
            if heartbeat_message.node_id == self.local_node_id:
                # _logger.debug("Ignoring own heartbeat message")
                return
            # Update node info manager with heartbeat
            NodesInfoManager.get_instance().handle_heartbeat(heartbeat_message, node_ip)
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


