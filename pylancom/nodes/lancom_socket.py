from __future__ import annotations

import time
import traceback
from asyncio import sleep as async_sleep
from json import dumps
from typing import Callable, Optional, TypeVar, cast

from .zmq_socket_manager import ZMQSocketManager

from ..utils.node_info import (
    LANCOM_PUB,
    LANCOM_SRV,
    HashIdentifier,
    SocketInfo,
    SocketTypeEnum,
)
from ..utils.log import logger
from ..utils.msg import send_bytes_request
from .lancom_base import LanComSocketBase
from .lancom_node import LanComNode

MessageT = TypeVar("MessageT", bytes, str, dict)
RequestT = TypeVar("RequestT", bytes, str, dict)
ResponseT = TypeVar("ResponseT", bytes, str, dict)


class Publisher(LanComSocketBase):
    def __init__(
        self,
        topic_name: str,
        with_local_namespace: bool = False,
    ):
        super().__init__(
            topic_name,
            LANCOM_PUB,
        )

    def publish_bytes(self, bytes_msg: bytes) -> None:
        # in case the publish too much messages
        self.loop_manager.submit_loop_task(
            self.send_bytes_async(bytes_msg), True
        )

    def publish_string(self, msg: str) -> None:
        self.publish_bytes(msg.encode())

    def publish_dict(self, data: dict) -> None:
        self.publish_string(dumps(data))

    def on_shutdown(self) -> None:
        self.zmq_socket.close()

    async def send_bytes_async(self, bytes_msg: bytes) -> None:
        # await self.socket.send(msg)
        await self.zmq_socket.send_multipart(
            [self.node.name.encode(), bytes_msg]
        )


class Streamer(Publisher):
    def __init__(
        self,
        topic_name: str,
        update_func: Callable[[], MessageT],
        fps: int,
        msg_encoder: Callable[[MessageT], bytes],
        start_streaming: bool = False,
    ):
        super().__init__(topic_name)
        self.running = False
        self.dt: float = 1 / fps
        self.update_func = update_func
        self.topic_byte = self.name.encode("utf-8")
        self.msg_encoder = msg_encoder
        if start_streaming:
            self.start_streaming()

    def start_streaming(self):
        self.loop_manager.submit_loop_task(self.update_loop(), False)

    def generate_byte_msg(self) -> bytes:
        return self.msg_encoder(self.update_func())
        # if isinstance(update_msg, str):
        #     return update_msg.encode("utf-8")
        # elif isinstance(update_msg, bytes):
        #     return update_msg
        # elif isinstance(update_msg, dict):
        #     # return dumps(update_msg).encode("utf-8")
        #     return dumps(
        #         {
        #             "updateData": self.update_func(),
        #             "time": time.monotonic(),
        #         }
        #     ).encode("utf-8")
        # raise ValueError("Update function should return str, bytes or dict")

    async def update_loop(self) -> None:
        self.running = True
        last = 0.0
        logger.info(f"Topic {self.name} starts streaming")
        while self.running:
            try:
                diff = time.monotonic() - last
                if diff < self.dt:
                    await async_sleep(self.dt - diff)
                last = time.monotonic()
                await self.send_bytes_async(self.generate_byte_msg())
            except Exception as e:
                logger.error(f"Error when streaming {self.name}: {e}")
                traceback.print_exc()
        logger.info(f"Streamer for topic {self.name} is stopped")


class Subscriber(LanComSocketBase):
    def __init__(
        self,
        topic_name: str,
        msg_decoder: Callable[[bytes], MessageT],
        callback: Callable[[MessageT], None],
    ):
        super().__init__(topic_name, SocketTypeEnum.SUBSCRIBER.value, False)
        self.socket = self.node.create_socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, self.name.encode())
        self.subscribed_components: dict[HashIdentifier, SocketInfo] = {}
        self.msg_decoder = msg_decoder
        self.connected = False
        self.callback = callback
        self.running = True
        self.loop_manager.submit_loop_task(self.listen_loop(), False)
        self.loop_manager.submit_loop_task(self.receive_loop(), False)

    async def receive_loop(self) -> None:
        """Listens for incoming messages on the subscribed topic."""
        logger.info(f"Subscriber {self.name} is subscribing ...")
        while self.running:
            try:
                # Wait for a message
                _, msg = await self.socket.recv_multipart()
                # Invoke the callback
                self.callback(self.msg_decoder(msg))
            except Exception as e:
                logger.error(f"Error from topic '{self.name}' subscriber: {e}")
                traceback.print_exc()

    async def listen_loop(self) -> None:
        """Listens for new publishers and connects to them."""
        logger.info(f"Subscriber {self.name} is listening ...")
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

    def connect(self, pub_info: SocketInfo) -> None:
        self.socket.connect(f"tcp://{pub_info['ip']}:{pub_info['port']}")
        self.subscribed_components[pub_info["socketID"]] = pub_info
        self.connected = True
        logger.info(
            f"Subscriber {self.name} is connected to {pub_info['name']}"
            f" from {pub_info['ip']}:{pub_info['port']}"
        )

    def on_shutdown(self) -> None:
        self.running = False
        self.socket.close()


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


# class ServiceProxy:
#     @staticmethod
#     def request(
#         service_name: str,
#         request_encoder: Callable[[RequestT], bytes],
#         response_decoder: Callable[[bytes], ResponseT],
#         request: RequestT,
#     ) -> Optional[ResponseT]:
#         if LanComNode.instance is None:
#             raise ValueError("Lancom Node is not initialized")
#         node = LanComNode.instance
#         service_component = node.nodes_map.get_service_info(service_name)
#         if service_component is None:
#             logger.warning(f"Service {service_name} is not exist")
#             return None
#         request_bytes = request_encoder(request)
#         addr = f"tcp://{service_component['ip']}:{service_component['port']}"
#         response = node.loop_manager.submit_loop_task(
#             send_bytes_request(addr, service_name, request_bytes),
#             True,
#         )
#         return response_decoder(cast(bytes, response))
