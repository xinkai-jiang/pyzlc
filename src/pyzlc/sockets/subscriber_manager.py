import traceback
from typing import Callable, List
import asyncio
import zmq
import msgpack

from ..nodes.zmq_socket_manager import ZMQSocketManager
from ..utils.log import _logger
from ..nodes.nodes_info_manager import NodesInfoManager
from ..nodes.loop_manager import LanComLoopManager


class Subscriber:
    """Subscribes to messages from a topic."""

    def __init__(
        self,
        topic_name: str,
        callback: Callable,
    ):
        self.nodes_manager = NodesInfoManager.get_instance()
        self.loop_manager = LanComLoopManager.get_instance()
        self._socket = ZMQSocketManager.get_instance().create_async_socket(zmq.SUB)
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.name = topic_name
        self.callback = callback
        self.running: bool = True
        self.connected: bool = False
        self.published_urls: List[str] = []
        self.loop_manager.submit_loop_task(self.listen_loop())

    def connect(self, url: str) -> None:
        """Connect to a publisher's socket."""
        self._socket.connect(url)
        self.connected = True
        self.published_urls.append(url)
        _logger.info("Subscriber %s is connected to %s", self.name, url)
        if len(self.published_urls) == 1:
            self.loop_manager.submit_loop_task(self.receive_loop())

    async def listen_loop(self) -> None:
        """Listens for new publishers and connects to them."""
        _logger.info("Subscriber %s is listening ...", self.name)
        while self.running:
            try:
                publishers = self.nodes_manager.get_publisher_info(self.name)
                for pub_info in publishers:
                    _url = f"tcp://{pub_info['ip']}:{pub_info['port']}"
                    if _url not in self.published_urls:
                        self.connect(_url)
                await asyncio.sleep(0.5)
            except Exception as e:
                _logger.error("Error from topic %s listener: %s", self.name, e)
                traceback.print_exc()
                raise e

    async def receive_loop(self) -> None:
        """Listens for incoming messages on the subscribed topic."""
        _logger.info("Subscriber %s is subscribing ...", self.name)
        while self.running:
            try:
                events = await self._socket.poll()
                if not events:
                    continue
                msg = await self._socket.recv()
                self.callback(msgpack.unpackb(msg))
            except Exception as e:
                _logger.error("Error from topic %s subscriber: %s", self.name, e)
                traceback.print_exc()
                raise e

    def close(self) -> None:
        """Close the subscriber socket."""
        self.running = False
        self._socket.close()
        _logger.info("Subscriber %s has been closed", self.name)


class SubscriberManager:
    """Manages multiple subscribers."""

    def __init__(self):
        self.subscribers: List[Subscriber] = []
        self.loop_manager = LanComLoopManager.get_instance()

    def add_subscriber(self, topic_name: str, callback: Callable) -> None:
        """Add a new subscriber and start its listening and receiving loops."""
        subscriber = Subscriber(topic_name, callback)
        self.subscribers.append(subscriber)

    def on_shutdown(self) -> None:
        """Shutdown all subscriber sockets."""
        for subscriber in self.subscribers:
            subscriber.close()
