from __future__ import annotations
from typing import Callable, Optional, Dict
import traceback
import asyncio
import zmq.asyncio
import msgpack

from zmq.asyncio import Socket as AsyncSocket
from ..utils.log import _logger
from ..nodes.loop_manager import LanComLoopManager
from ..utils.msg import get_socket_addr, RequestT, ResponseT, ResponseStatus

HandlerFunc = Callable[[RequestT], ResponseT]
ServiceCallback = Callable[[bytes], Optional[bytes]]


class ServiceManager:
    """Manages services using a REP socket."""

    def __init__(self, url: str) -> None:
        """Initialize the ServiceManager with a REP socket."""
        self.res_socket: AsyncSocket = zmq.asyncio.Context.instance().socket(zmq.REP)
        self.callable_services: Dict[str, ServiceCallback] = {}
        self.res_socket.bind(url)
        url, self.port = get_socket_addr(self.res_socket)
        _logger.info("ServiceManager REP socket bound to %s", url)
        self._running: bool = True
        self.loop_manager = LanComLoopManager.get_instance()
        self.service_loop_future = self.loop_manager.submit_loop_task(
            self.service_loop(self.res_socket, self.callable_services)
        )

    @staticmethod
    def _wrap_handler(handler: HandlerFunc) -> ServiceCallback:
        """Static helper to wrap a standard handler with msgpack logic."""

        def wrapper(request_bytes: bytes) -> bytes:
            # Unpack the request
            try:
                arg = msgpack.unpackb(request_bytes, raw=False)
                result = msgpack.packb(handler(arg), use_bin_type=True)
                assert result is not None, "msgpack.packb returned None"
                return result
            except msgpack.ExtraData as e:
                _logger.error(f"Message unpacking error: {e}")
                return b""

        return wrapper

    def register_service(self, service_name: str, handler: HandlerFunc) -> None:
        """Register a service with a given name and handler function."""
        self.callable_services[service_name] = self._wrap_handler(handler)
        _logger.info(f"Service '{service_name}' registered successfully.")

    async def service_loop(
        self,
        _socket: AsyncSocket,
        services: dict[str, ServiceCallback],
    ) -> None:
        """Asynchronously handles incoming service requests."""
        while self._running:
            try:
                event = await _socket.poll()
                if not event:
                    continue
                name_bytes, request = await _socket.recv_multipart()
            except Exception as e:
                _logger.error(f"Error occurred when receiving request: {e}")
                traceback.print_exc()
                raise e
            service_name = name_bytes.decode()
            # _logger.debug(f"Received request for service: {service_name}")
            if service_name not in services.keys():
                _logger.error(f"Service {service_name} is not available")
                continue
            response_status: bytes = ResponseStatus.SUCCESS.encode()
            packed_result: Optional[bytes] = None
            try:
                packed_result = await asyncio.wait_for(
                    self.loop_manager.run_in_executor(services[service_name], request),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                _logger.error("Timeout: callback function took too long")
                response_status = ResponseStatus.SERVICE_TIMEOUT.encode()
            except msgpack.ExtraData as e:
                _logger.error(f"Message unpacking error: {e}")
                response_status = ResponseStatus.INVALID_REQUEST.encode()
            except Exception as e:
                _logger.error(
                    f"One error occurred when processing the Service {service_name}: {e}"
                )
                response_status = ResponseStatus.UNKNOWN_ERROR.encode()
                traceback.print_exc()
                raise e
            try:
                await _socket.send_multipart([response_status, packed_result or b""])
            except Exception as e:
                _logger.error(f"Error occurred when sending response: {e}")
                traceback.print_exc()
                raise e
        _logger.info("Service loop has been stopped")

    def stop(self) -> None:
        """Stop the service manager and close the socket."""
        self._running = False
        self.res_socket.close()
        self.service_loop_future.cancel()
        _logger.info("ServiceManager has been stopped")
