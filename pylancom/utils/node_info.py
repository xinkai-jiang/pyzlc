from enum import Enum
from typing import TypedDict, List
import zmq
import zmq.asyncio

IPAddress = str
Port = int
TopicName = str
ServiceName = str
AsyncSocket = zmq.asyncio.Socket
HashIdentifier = str

LANCOM_PUB = zmq.PUB
LANCOM_SUB = zmq.SUB
LANCOM_SRV = zmq.REQ


class LanComMsg(Enum):
    """Enumeration of LanCom message types."""
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"
    EMPTY = "EMPTY"


class SocketInfo(TypedDict):
    """Information about a socket."""
    name: str
    socketID: HashIdentifier
    nodeID: HashIdentifier
    type: int
    address: IPAddress


class NodeInfo(TypedDict):
    """Information about a node."""
    name: str
    nodeID: HashIdentifier
    infoID: int
    ip: IPAddress
    port: Port
    topics: List[SocketInfo]
    services: List[SocketInfo]
    
    
# class SocketInfo(TypedDict):
#     """Information about a socket."""
#     name: str 
#     port: int


# class NodeInfo(TypedDict):
#     """Information about a node."""
#     name: str
#     nodeID: str
#     infoID: int
#     ip: IPAddress
#     port: int
#     topics: List[SocketInfo]
#     services: List[SocketInfo]