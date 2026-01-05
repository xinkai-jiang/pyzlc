from typing import List, TypedDict
import struct


HashIdentifier = str
TopicName = str
ServiceName = str

# =======================
# SocketInfo & NodeInfo
# =======================

class SocketInfo(TypedDict):
    """Socket information structure."""
    name: str
    ip: str
    port: int


class NodeInfo(TypedDict):
    """Node information structure."""
    nodeID: str
    infoID: int
    name: str
    ip: str
    topics: List[SocketInfo]
    services: List[SocketInfo]

class BinWriter:
    def __init__(self):
        self.buf: bytearray = bytearray()

    def write_u16(self, v: int):
        # ">H" is Big-endian unsigned short (2 bytes)
        self.buf.extend(struct.pack(">H", v))

    def write_u32(self, v: int):
        # ">I" is Big-endian unsigned int (4 bytes)
        self.buf.extend(struct.pack(">I", v))

    def write_string(self, s: str):
        bs = s.encode("utf-8")
        self.write_u16(len(bs))
        self.buf.extend(bs)

    def write_fixed_string(self, s: str, length: int):
        # Direct extend is faster than checking length in Python
        # if you are sure about the data source.
        self.buf.extend(s.encode("utf-8"))

class BinReader:
    def __init__(self, data: bytes):
        self.view = memoryview(data)
        self.pos = 0

    def read_u16(self) -> int:
        # Unpack_from avoids creating a new slice of memory
        v = struct.unpack_from(">H", self.view, self.pos)[0]
        self.pos += 2
        return v

    def read_u32(self) -> int:
        v = struct.unpack_from(">I", self.view, self.pos)[0]
        self.pos += 4
        return v

    def read_string(self) -> str:
        length = self.read_u16()
        return self.read_fixed_string(length)

    def read_fixed_string(self, length: int) -> str:
        s = bytes(self.view[self.pos : self.pos + length]).decode("utf-8")
        self.pos += length
        return s

# Optimized Serialization Flow
def encode_socket_info_to(info: SocketInfo, writer: BinWriter):
    """Writes directly into an existing writer to avoid allocations."""
    writer.write_string(info["name"])
    writer.write_u16(info["port"])

def encode_node_info(info: NodeInfo) -> bytes:
    writer = BinWriter()
    writer.write_fixed_string(info["nodeID"], 36)
    writer.write_u32(info["infoID"])
    writer.write_string(info["name"])
    writer.write_string(info["ip"])

    writer.write_u16(len(info["topics"]))
    for topic in info["topics"]:
        encode_socket_info_to(topic, writer) # No new writer created

    writer.write_u16(len(info["services"]))
    for service in info["services"]:
        encode_socket_info_to(service, writer)

    return bytes(writer.buf)


def decode_socket_info(reader: BinReader, ip: str) -> SocketInfo:
    """Decode SocketInfo from bytes."""
    name = reader.read_string()
    port = reader.read_u16()
    return SocketInfo(name=name, ip=ip, port=port)

def decode_node_info(data: bytes) -> NodeInfo:
    """Decode NodeInfo from bytes."""
    reader = BinReader(data)
    node_id = reader.read_fixed_string(36)
    info_id = reader.read_u32()
    name = reader.read_string()
    ip = reader.read_string()

    topics_count = reader.read_u16()
    topics: List[SocketInfo] = []
    for _ in range(topics_count):
        topics.append(decode_socket_info(reader, ip))

    services_count = reader.read_u16()
    services: List[SocketInfo] = []
    for _ in range(services_count):
        services.append(decode_socket_info(reader, ip))

    return NodeInfo(
        nodeID=node_id,
        infoID=info_id,
        name=name,
        ip=ip,
        topics=topics,
        services=services
    )
