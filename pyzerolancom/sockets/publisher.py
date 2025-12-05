from __future__ import annotations

import time
import traceback
from asyncio import sleep as async_sleep
from typing import Callable, Dict

import zmq
import msgpack

from ..nodes.zmq_socket_manager import ZMQSocketManager
from ..utils.log import logger
from ..nodes.lancom_node import LanComNode
from ..nodes.loop_manager import LanComLoopManager



class Publisher:
    """Publishes messages to a topic."""
    def __init__(
        self,
        topic_name: str,
        with_local_namespace: bool = False,
    ):
        if LanComNode.instance is None:
            raise ValueError("Lancom Node is not initialized")
        if with_local_namespace:
            self.name = f"{LanComNode.instance.name}/{topic_name}"
        else:
            self.name = topic_name
        self._socket = ZMQSocketManager.get_instance().create_socket(zmq.PUB)
        self.loop_manager = LanComLoopManager.get_instance()

    def publish(self, msg: Dict) -> None:
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
        update_func: Callable[[], Dict],
        fps: int,
        start_streaming: bool = False,
        with_local_namespace: bool = False,
    ):
        if LanComNode.instance is None:
            raise ValueError("Lancom Node is not initialized")
        if with_local_namespace:
            self.name = f"{LanComNode.instance.name}/{topic_name}"
        else:
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
        self.loop_manager.submit_loop_task(self.update_loop(), False)

    async def update_loop(self) -> None:
        """Streams messages at the specified rate."""
        self.running = True
        last = 0.0
        logger.info("Topic %s starts streaming", self.name)
        while self.running:
            try:
                diff = time.monotonic() - last
                if diff < self.dt:
                    await async_sleep(self.dt - diff)
                last = time.monotonic()
                self._socket.send(msgpack.packb(self.update_func()))
            except Exception as e:
                logger.error("Error when streaming %s: %s", self.name, e)
                traceback.print_exc()
        logger.info("Streamer for topic %s is stopped", self.name)




# class Service(LanComSocketBase):
#     def __init__(
#         self,
#         service_name: str,
#         request_decoder: Callable[[bytes], RequestT],
#         response_encoder: Callable[[ResponseT], bytes],
#         callback: Callable[[RequestT], ResponseT],
#     ) -> None:
#         super().__init__(service_name, LANCOM_SRV, False)
#         # check the service is already registered locally
#         for service_info in self.node.local_info["srvList"]:
#             if service_info["name"] != self.name:
#                 continue
#             raise RuntimeError("Service has been registered locally")
#         if self.node.nodes_map.get_service_info(service_name) is not None:
#             raise RuntimeError("Service has been registered")
#         self.node.local_info["srvList"].append(self.info)
#         self.node.local_info["infoID"] += 1
#         self.handle_request = callback
#         self.request_decoder = request_decoder
#         self.response_encoder = response_encoder
#         logger.info(f'"{self.name}" Service is started')

#     def callback(self, msg: bytes) -> bytes:
#         request = self.request_decoder(msg)
#         result = self.handle_request(request)
#         return self.response_encoder(result)

#     def on_shutdown(self):
#         self.node.local_info["srvList"].remove(self.info)
#         logger.info(f'"{self.name}" Service is stopped')



