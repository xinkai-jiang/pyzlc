from __future__ import annotations
import select
import socket
import struct
import threading
import traceback
from typing import Tuple, Optional

from ..nodes.nodes_info_manager import NodesInfoManager
from ..utils.log import _logger
from ..utils.msg import (
    _parse_version,
    is_in_same_subnet,
    HeartbeatMessage,
    decode_heartbeat_message,
)
from ..utils.node_info import NodeInfo
from .loop_manager import LanComLoopManager


class MulticastWorker:

    def __init__(
        self,
        local_info: NodeInfo,
        service_port: int,
        group: str,
        group_port: int,
        group_name: str,
    ) -> None:
        self.local_info = local_info
        self.local_ip = local_info["ip"]
        self.service_port = service_port
        self.group = group
        self.group_port = group_port
        self.group_name = group_name
        self.protocol_version: Tuple[int, int, int] = _parse_version()

        # Threading support
        self._stop_event = threading.Event()
        self._sender_thread: Optional[threading.Thread] = None
        self._receiver_thread: Optional[threading.Thread] = None
        self.loop_manager: Optional[LanComLoopManager] = None
        self.node_info_manager: Optional[NodesInfoManager] = None

    def start(self):
        """Start the multicast worker with separate threads for sending and receiving."""
        self._stop_event.clear()
        self.loop_manager = LanComLoopManager.get_instance()
        self.node_info_manager = NodesInfoManager.get_instance()

        self._sender_thread = threading.Thread(
            target=self._multicast_loop, name="MulticastSender", daemon=True
        )
        self._receiver_thread = threading.Thread(
            target=self._discovery_loop, name="MulticastReceiver", daemon=True
        )

        self._sender_thread.start()
        self._receiver_thread.start()
        _logger.debug("Multicast worker started")

    def stop(self):
        """Stop the multicast worker and wait for threads to finish."""
        _logger.debug("Stopping multicast worker...")
        self._stop_event.set()

        if self._sender_thread and self._sender_thread.is_alive():
            self._sender_thread.join(timeout=2.0)
        if self._receiver_thread and self._receiver_thread.is_alive():
            self._receiver_thread.join(timeout=2.0)

        _logger.debug("Multicast worker stopped")

    def _multicast_loop(self, interval: float = 0.5):
        """Send multicast heartbeat messages at regular intervals (runs in thread)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.setsockopt(
            socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.local_ip)
        )
        _service_port = self.service_port

        while not self._stop_event.is_set():
            try:
                msg = HeartbeatMessage(
                    zlc_version=self.protocol_version,
                    node_id=self.local_info["nodeID"],
                    info_id=self.local_info["infoID"],
                    service_port=_service_port,
                    group_name=self.group_name,
                )
                sock.sendto(msg.to_bytes(), (self.group, self.group_port))
                # Wait for interval or until stop is signaled
                self._stop_event.wait(timeout=interval)
            except Exception as e:
                _logger.error("Error in multicast loop: %s", e)
                traceback.print_exc()
                break

        sock.close()
        _logger.debug("Multicast heartbeat loop stopped")

    def _discovery_loop(self):
        """Listen for multicast discovery messages and register nodes (runs in thread)."""
        sock: Optional[socket.socket] = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(("", self.group_port))
            mreq = struct.pack(
                "4s4s",
                socket.inet_aton(self.group),
                socket.inet_aton(self.local_ip),
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            _logger.debug(
                "Listening for multicast on %s:%s", self.group, self.group_port
            )

            while not self._stop_event.is_set():
                # Use select to wait for data with timeout
                readable, _, _ = select.select([sock], [], [], 0.5)
                if not readable:
                    continue

                try:
                    data, addr = sock.recvfrom(1024)
                    self._handle_datagram(data, addr)
                except socket.error as e:
                    if not self._stop_event.is_set():
                        _logger.error("Socket error in discovery loop: %s", e)
        except Exception as e:
            _logger.error("Error in discovery loop: %s", e)
            traceback.print_exc()
        finally:
            if sock:
                try:
                    sock.close()
                except Exception as e:
                    _logger.error("Error closing discovery socket: %s", e)
            _logger.debug("Multicast discovery loop stopped")

    def _handle_datagram(self, data: bytes, addr: Tuple[str, int]):
        """Handle incoming multicast discovery messages."""
        try:
            node_ip = addr[0]
            if not is_in_same_subnet(self.local_ip, node_ip):
                return

            heartbeat_message = decode_heartbeat_message(data)
            if (
                heartbeat_message is None
                or heartbeat_message.group_name != self.group_name
            ):
                return

            if heartbeat_message.node_id == self.local_info["nodeID"]:
                return
            if self.loop_manager is None or self.node_info_manager is None:
                _logger.warning(
                    "Loop manager or node info manager not initialized yet, skipping datagram"
                )
                return
            # Submit async task to event loop (preserving async chain)
            self.loop_manager.submit_loop_task(
                self.node_info_manager.handle_heartbeat_async(
                    heartbeat_message, node_ip
                )
            )
        except Exception as e:
            _logger.error("Error processing datagram: %s", e)
            traceback.print_exc()
