from __future__ import annotations
import time
import traceback
from asyncio import sleep as async_sleep
from typing import Callable

import zmq
import msgpack

from ..nodes.zmq_socket_manager import ZMQSocketManager
from ..utils.log import _logger
from ..nodes.nodes_info_manager import LocalNodeInfo
from ..nodes.loop_manager import LanComLoopManager
from ..utils.msg import MessageT, get_socket_addr


class Publisher:
    """Publishes messages to a topic."""

    def __init__(self, topic_name: str):
        self.name = topic_name
        self.loop_manager = LanComLoopManager.get_instance()
        local_node_info = LocalNodeInfo.get_instance()
        self._socket = ZMQSocketManager.get_instance().create_socket(zmq.PUB)
        self._socket.bind(f"tcp://{local_node_info.node_info['ip']}:0")
        _, self.port = get_socket_addr(self._socket)
        local_node_info.register_publisher(self.name, self.port)

    def publish(self, msg: MessageT) -> None:
        """Publish a message in bytes."""
        msgpacked = msgpack.packb(msg)
        self._socket.send(msgpacked)

    def on_shutdown(self) -> None:
        """Shutdown the publisher socket."""
        self._socket.close()


class Streamer:
    """Streams messages to a topic at a fixed rate."""

    def __init__(
        self,
        topic_name: str,
        update_func: Callable[[], MessageT],
        fps: int,
        start_streaming: bool = False,
    ):
        self.name = topic_name
        self._socket = ZMQSocketManager.get_instance().create_async_socket(zmq.PUB)
        self.loop_manager = LanComLoopManager.get_instance()
        self.running = False
        self.dt: float = 1 / fps
        self.update_func = update_func
        if start_streaming:
            self.start_streaming()

    def start_streaming(self):
        """Start the streaming loop."""
        self.loop_manager.submit_loop_task(self.update_loop())

    async def update_loop(self) -> None:
        """Streams messages at the specified rate."""
        self.running = True
        last = 0.0
        _logger.info("Topic %s starts streaming", self.name)
        while self.running:
            try:
                diff = time.monotonic() - last
                if diff < self.dt:
                    await async_sleep(self.dt - diff)
                last = time.monotonic()
                self._socket.send(msgpack.packb(self.update_func()))
            except Exception as e:
                _logger.error("Error when streaming %s: %s", self.name, e)
                traceback.print_exc()
                raise e
        _logger.info("Streamer for topic %s is stopped", self.name)
