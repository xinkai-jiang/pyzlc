from __future__ import annotations
from typing import Callable, Any
import traceback
import inspect
import asyncio
import zmq.asyncio
import msgpack

from zmq.asyncio import Socket as AsyncSocket
from ..utils.log import _logger
from ..nodes.loop_manager import LanComLoopManager
from ..utils.msg import get_socket_addr


def non_argument_func_wrapper(func: Callable) -> Callable:
    """Wrap a function with no arguments to accept bytes and return bytes."""

    def wrapper(_: bytes):
        result = func()
        if result is None:
            return b""
        return msgpack.packb(result, use_bin_type=True)

    return wrapper


def argument_func_wrapper(func: Callable) -> Callable:
    """Wrap a function with one argument to accept bytes and return bytes."""

    def wrapper(request_bytes: bytes):
        arg = msgpack.unpackb(request_bytes, raw=False)
        result = func(arg)
        if result is None:
            return b""
        return msgpack.packb(result, use_bin_type=True)

    return wrapper

Empty = type(None)
empty = None

class ServiceManager:
    """Manages services using a REP socket."""

    def __init__(self, url: str) -> None:
        """Initialize the ServiceManager with a REP socket."""
        self.services: dict[str, dict] = {}
        self.res_socket: AsyncSocket = zmq.asyncio.Context().socket(zmq.REP)
        self.callable_services: dict[str, Callable[[bytes], bytes]] = {}
        self.res_socket.bind(url)
        url, self.port = get_socket_addr(self.res_socket)
        _logger.info("ServiceManager REP socket bound to %s", url)
        self._running: bool = True
        self.loop_manager = LanComLoopManager.get_instance()
        self.service_loop_future = self.loop_manager.submit_loop_task(
            self.service_loop(self.res_socket, self.callable_services)
        )

    def register_service(self, service_name: str, handler: Callable) -> None:
        """Register a service with a given name and handler function."""
        sig = inspect.signature(handler)
        params = list(sig.parameters.values())
        if len(params) == 0:
            self.callable_services[service_name] = non_argument_func_wrapper(handler)
        elif len(params) == 1:
            self.callable_services[service_name] = argument_func_wrapper(handler)
        else:
            raise TypeError(
                f"Service '{service_name}' handler must have 0 or 1 parameter."
            )
        _logger.info(f"Service '{service_name}' registered successfully.")

    async def service_loop(
        self,
        service_socket: zmq.asyncio.Socket,
        services: dict[str, Callable[[Any], Any]],
    ) -> None:
        """Asynchronously handles incoming service requests."""
        while self._running:
            try:
                event = await service_socket.poll()
                if not event:
                    continue
                name_bytes, request = await service_socket.recv_multipart()
            except Exception as e:
                _logger.error(f"Error occurred when receiving request: {e}")
                traceback.print_exc()
                raise e
            service_name = name_bytes.decode()
            if service_name not in services.keys():
                _logger.error(f"Service {service_name} is not available")
                continue
            try:
                result = await asyncio.wait_for(
                    self.loop_manager.run_in_executor(services[service_name], request),
                    timeout=2.0,
                )
                packed_result = msgpack.packb(result, use_bin_type=True)
                await service_socket.send(packed_result)
            except asyncio.TimeoutError:
                _logger.error("Timeout: callback function took too long")
                await service_socket.send_string("TIMEOUT")
            except msgpack.ExtraData as e:
                _logger.error(f"Message unpacking error: {e}")
                await service_socket.send_string("UNPACKING_ERROR")
            except Exception as e:
                _logger.error(
                    f"One error occurred when processing the Service {service_name}: {e}"
                )
                traceback.print_exc()
                await service_socket.send_string("ERROR")
                raise e
        _logger.info("Service loop has been stopped")

    def stop(self) -> None:
        """Stop the service manager and close the socket."""
        self._running = False
        self.res_socket.close()
        self.service_loop_future.cancel()
        _logger.info("ServiceManager has been stopped")
