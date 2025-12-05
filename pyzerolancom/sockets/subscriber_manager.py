import traceback
from typing import Callable, Dict, List
import zmq.asyncio
import msgpack

from ..nodes.zmq_socket_manager import ZMQSocketManager
from ..utils.log import logger
from ..nodes.lancom_node import LanComNode
from ..nodes.loop_manager import LanComLoopManager
from ..utils.node_info import SocketInfo

class Subscriber:
    """Subscribes to messages from a topic."""
    def __init__(
        self,
        topic_name: str,
        callback: Callable,
    ):
        self._socket = ZMQSocketManager.get_instance().create_async_socket(zmq.SUB)
        self.name = topic_name
        self.callback = callback
        self.running: bool = True
        self.connected: bool = False
        self.published_urls: List[str] = []
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")

    def connect(self, pub_info: SocketInfo) -> None:
        """Connect to a publisher's socket."""
        _url = f"tcp://{pub_info['ip']}:{pub_info['port']}"
        self._socket.connect(_url)
        self.connected = True
        self.published_urls.append(_url)
        logger.info(
            f"Subscriber {self.name} is connected to {pub_info['name']}"
            f" from {_url}"
        )

    async def receive_loop(self) -> None:
        """Listens for incoming messages on the subscribed topic."""
        logger.info(f"Subscriber {self.name} is subscribing ...")
        while self.running:
            try:
                # Wait for a message
                msg = await self._socket.recv()
                # Invoke the callback
                self.callback(msgpack.unpackb(msg))
            except Exception as e:
                logger.error(f"Error from topic '{self.name}' subscriber: {e}")
                traceback.print_exc()


class SubscriberManager:
    def __init__(self):
        self.loop_manager = LanComLoopManager.get_instance()
        self.loop_manager.submit_loop_task(self.listen_loop(), False)
        self.loop_manager.submit_loop_task(self.receive_loop(), False)

    async def listen_loop(self, url: str) -> None:
        """Listens for new publishers and connects to them."""
        logger.info(f"Subscriber {self.name} is listening ...")
        socket_ = ZMQSocketManager.get_instance().create_async_socket(zmq.SUB)
        while self.running:
            try:
                publishers = self.node.nodes_map.get_publisher_info(self.name)
                # print(f"Publishers: {publishers}")
                for pub_info in publishers:
                    if pub_info["socketID"] not in self.subscribed_components:
                        self.connect(pub_info)
                await async_sleep(0.5)
            except Exception as e:
                logger.error(f"Error from topic '{self.name}' listener: {e}")
                traceback.print_exc()

    def on_shutdown(self) -> None:
        self.running = False
        self.socket.close()
