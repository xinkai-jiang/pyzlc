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

    async def _handle_request(
        self, service_name: str, request: bytes, services: dict[str, ServiceCallback]
    ) -> tuple[bytes, bytes]:
        """Handle a single service request and return (status, result)."""
        if service_name not in services:
            _logger.error(f"Service {service_name} is not available")
            return ResponseStatus.NOSERVICE.encode(), b""
        try:
            result = await asyncio.wait_for(
                self.loop_manager.run_in_executor(services[service_name], request),
                timeout=2.0,
            )
            return ResponseStatus.SUCCESS.encode(), result or b""
        except (asyncio.CancelledError, RuntimeError) as e:
            if not self._running:
                raise  # Re-raise to exit the loop
            _logger.error(f"Error in service {service_name}: {e}")
            return ResponseStatus.UNKNOWN_ERROR.encode(), b""
        except asyncio.TimeoutError:
            _logger.error("Timeout: callback function took too long")
            return ResponseStatus.SERVICE_TIMEOUT.encode(), b""
        except msgpack.ExtraData as e:
            _logger.error(f"Message unpacking error: {e}")
            return ResponseStatus.INVALID_REQUEST.encode(), b""
        except Exception as e:
            _logger.error(f"Error processing service {service_name}: {e}")
            traceback.print_exc()
            return ResponseStatus.UNKNOWN_ERROR.encode(), b""

    async def service_loop(
        self,
        _socket: AsyncSocket,
        services: dict[str, ServiceCallback],
    ) -> None:
        """Asynchronously handles incoming service requests."""
        while self._running:
            try:
                if not await _socket.poll(timeout=100):
                    continue
                name_bytes, request = await _socket.recv_multipart()
                status, result = await self._handle_request(name_bytes.decode(), request, services)
                await _socket.send_multipart([status, result])
            except asyncio.CancelledError:
                break
            except (RuntimeError, Exception) as e:
                if not self._running:
                    break
                _logger.error(f"Service loop error: {e}")
                traceback.print_exc()
        _logger.info("Service loop has been stopped")

    def stop(self) -> None:
        """Stop the service manager and close the socket."""
        self._running = False
        self.res_socket.close()
        self.service_loop_future.cancel()
        _logger.info("ServiceManager has been stopped")
