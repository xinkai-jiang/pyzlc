import asyncio
import socket
import struct
import uuid
from typing import Optional, Union, Dict, Tuple, List, Final, TypedDict
from typing_extensions import TypeAlias
import traceback

import ipaddress
import zmq
import zmq.asyncio
import msgpack

from .node_info import HashIdentifier
from .log import _logger


def _get_zlc_version() -> str:
    """Get ZLC version from package metadata."""
    import importlib.metadata

    try:
        version_str = importlib.metadata.version("pyzlc")
    except importlib.metadata.PackageNotFoundError:
        version_str = "unknown"
    return version_str


def _parse_version() -> Tuple[int, int, int]:
    """Parse a version string like '1.2.3' into a tuple (1, 2, 3).

    Handles unknown/invalid versions by returning (0, 0, 0).
    """
    version_str = _get_zlc_version()
    if version_str == "unknown":
        return (0, 0, 0)
    try:
        parts = version_str.split(".")
        major = int(parts[0]) if len(parts) > 0 else 0
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2].split("-")[0].split("+")[0]) if len(parts) > 2 else 0
        return (major, minor, patch)
    except (ValueError, IndexError):
        return (0, 0, 0)


Empty: TypeAlias = None
empty = None
MessageT = Union[TypedDict, Dict, str]
RequestT = Union[TypedDict, Dict, str, Empty]
ResponseT = Union[TypedDict, Dict, str, Empty]


class ResponseStatus:
    """
    Namespace for service response status strings.
    Matches the C++ ZeroLanCom implementation.
    """

    SUCCESS: Final[str] = "SUCCESS"
    NOSERVICE: Final[str] = "NOSERVICE"
    INVALID_RESPONSE: Final[str] = "INVALID_RESPONSE"
    SERVICE_FAIL: Final[str] = "SERVICE_FAIL"
    SERVICE_TIMEOUT: Final[str] = "SERVICE_TIMEOUT"
    INVALID_REQUEST: Final[str] = "INVALID_REQUEST"
    UNKNOWN_ERROR: Final[str] = "UNKNOWN_ERROR"

    @staticmethod
    def is_error(status: str) -> bool:
        """Helper to validate incoming status strings."""
        return status != ResponseStatus.SUCCESS


class HeartbeatMessage:
    """Typed tuple for heartbeat message components.

    Components:
        - zlc_version: Tuple[int, int, int]
        - node_id: str
        - info_id: int
        - service_port: int
        - group_name: str
    """

    def __init__(
        self,
        zlc_version: Tuple[int, int, int],
        node_id: str,
        info_id: int,
        service_port: int,
        group_name: str,
    ) -> None:
        self.zlc_version: Tuple[int, int, int] = zlc_version
        self.node_id: str = node_id
        self.info_id: int = info_id
        self.service_port: int = service_port
        self.group_name: str = group_name

    def to_bytes(self) -> bytes:
        """Serialize the heartbeat message to bytes."""
        msg = bytearray(
            struct.pack(
                "!3i36s2i",
                *self.zlc_version,
                self.node_id.encode("utf-8"),
                self.info_id,
                self.service_port,
            )
        )
        msg.extend(self.group_name.encode("utf-8"))
        return bytes(msg)


def decode_heartbeat_message(data: bytes) -> HeartbeatMessage:
    """Decode the binary heartbeat message.

    Structure:
        - zlc_version: 3 x int32 (12 bytes)
        - node_id: 36 bytes string
        - info_id: int32 (4 bytes)
        - service_port: int32 (4 bytes)
        - group_name: remaining bytes
    """
    if len(data) < 56:
        raise ValueError(
            f"Message too short: expected at least 56 bytes, got {len(data)}"
        )
    unpacked_data = struct.unpack("!3i36s2i", data[:56])
    major, minor, patch = unpacked_data[0:3]
    node_id = unpacked_data[3].decode("utf-8").strip("\x00")
    info_id = unpacked_data[4]
    service_port = unpacked_data[5]
    group_name = data[56:].decode("utf-8")

    return HeartbeatMessage(
        (major, minor, patch), node_id, info_id, service_port, group_name
    )


def is_in_same_subnet(ip1: str, ip2: str, subnet_mask: str = "255.255.255.0") -> bool:
    """
    Determines if two IP addresses reside within the same logical subnet.

    Args:
        ip1: The first IP address (e.g., the local node IP).
        ip2: The second IP address (e.g., the discovered remote IP).
        subnet_mask: The mask defining the subnet range (default is /24).

    Returns:
        bool: True if both IPs share the same network prefix, False otherwise.
    """
    try:
        # Create a network object using the first IP and the mask.
        # strict=False allows the use of a host IP instead of a network address.
        network = ipaddress.ip_network(f"{ip1}/{subnet_mask}", strict=False)

        # Check if the second IP address is contained within that network.
        return ipaddress.ip_address(ip2) in network
    except ValueError as e:
        # Log error if IP format or mask is invalid.
        print(f"Error validating IP subnet: {e}")
        return False


def create_hash_identifier() -> HashIdentifier:
    """
    Generate a unique hash identifier.
    This function creates a new UUID (Universally Unique Identifier).
    Returns:
        HashIdentifier: A unique hash identifier in string format.
        The hash identifier is 36 characters long.
    """

    return str(uuid.uuid4())


def get_socket_addr(
    zmq_socket: Union[zmq.Socket, zmq.asyncio.Socket],
) -> Tuple[str, int]:
    """Get the address and port of a ZMQ socket."""
    endpoint: bytes = zmq_socket.getsockopt(zmq.LAST_ENDPOINT)  # type: ignore
    return endpoint.decode(), int(endpoint.decode().split(":")[-1])


def calculate_broadcast_addr(ip_addr: str) -> str:
    """Calculate the broadcast address for a given IP address."""
    ip_bin = struct.unpack("!I", socket.inet_aton(ip_addr))[0]
    netmask_bin = struct.unpack("!I", socket.inet_aton("255.255.255.0"))[0]
    broadcast_bin = ip_bin | ~netmask_bin & 0xFFFFFFFF
    return socket.inet_ntoa(struct.pack("!I", broadcast_bin))


async def send_bytes_request(
    addr: str, service_name: str, bytes_msgs: bytes, timeout: float
) -> Optional[List[bytes]]:
    """Send a bytes request to the specified address and return the response."""
    try:
        sock = zmq.asyncio.Context.instance().socket(zmq.REQ)
        sock.connect(addr)
        # Send the message; you can also wrap this in wait_for if needed.
        await sock.send_multipart([service_name.encode(), bytes_msgs])
        # Wait for a response with a timeout.
        response = await asyncio.wait_for(sock.recv_multipart(), timeout=timeout)
        assert len(response) == 2, "Invalid response format"
    except asyncio.TimeoutError:
        _logger.error("Request %s timed out for %s s.", service_name, timeout)
        response = None
    except Exception as e:
        _logger.error("Error sending request to %s: %s", addr, e)
        traceback.print_exc()
        response = None
    finally:
        sock.disconnect(addr)
        sock.close()
        return response


async def send_request(
    addr: str, service_name: str, request: RequestT, timeout: float
) -> Optional[ResponseT]:
    """Send a request to the specified address and return the response."""
    if isinstance(request, bytes):
        request_bytes = request
    else:
        request_bytes = msgpack.packb(request, use_bin_type=True)
    if request_bytes is None:
        _logger.error("Failed to pack request for service %s", service_name)
        return None
    response = await send_bytes_request(addr, service_name, request_bytes, timeout)
    if response is None:
        return None
    if response[0].decode() != ResponseStatus.SUCCESS:
        _logger.error(
            "Service %s returned error status: %s",
            service_name,
            response[0].decode(),
        )
        return None
    return msgpack.unpackb(response[1])
