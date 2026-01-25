from __future__ import annotations
from typing import Optional
import zmq.asyncio

PUB = zmq.PUB
SUB = zmq.SUB
REQ = zmq.REQ
REP = zmq.REP


class ZMQSocketManager:
    """Singleton class to manage ZMQ sockets."""

    _instance: Optional[ZMQSocketManager] = None

    @classmethod
    def get_instance(cls) -> ZMQSocketManager:
        """Get the singleton instance of ZMQSocketManager."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        ZMQSocketManager._instance = self
        self.context: zmq.Context = zmq.Context()
        self.async_context: zmq.asyncio.Context = zmq.asyncio.Context.instance()

    def create_socket(self, socket_type: int) -> zmq.Socket:
        """Create and return a new ZMQ socket of the specified type."""
        return self.context.socket(socket_type)

    def create_async_socket(self, socket_type: int) -> zmq.asyncio.Socket:
        """Create and return a new asynchronous ZMQ socket of the specified type."""
        return self.async_context.socket(socket_type)

    def shutdown(self) -> None:
        """Shutdown all ZMQ contexts and cleanup resources."""
        self.context.term()
        self.async_context.term()
