import asyncio
import socket
import struct
import uuid
from typing import Optional, Union, Dict, Tuple, List, Final, TypedDict

import zmq
import zmq.asyncio

from .node_info import HashIdentifier
from .log import _logger

Empty = type(None)
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
    addr: str, service_name: str, bytes_msgs: bytes, timeout: float = 1.0
) -> Optional[List[bytes]]:
    """Send a bytes request to the specified address and return the response."""
    try:
        sock = zmq.asyncio.Context().socket(zmq.REQ)
        sock.connect(addr)
        # Send the message; you can also wrap this in wait_for if needed.
        await sock.send_multipart([service_name.encode(), bytes_msgs])
        # Wait for a response with a timeout.
        response = await asyncio.wait_for(sock.recv_multipart(), timeout=timeout)
        assert len(response) != 2, "Invalid response format"
    except asyncio.TimeoutError:
        _logger.error("Request %s timed out for %s s.", service_name, timeout)
        response = None
    finally:
        sock.disconnect(addr)
        sock.close()
        return response
