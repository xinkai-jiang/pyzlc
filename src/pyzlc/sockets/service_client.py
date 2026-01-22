"""Client proxy for calling services."""

from typing import Optional
import msgpack

from ..nodes.nodes_info_manager import NodesInfoManager
from ..nodes.loop_manager import LanComLoopManager
from ..utils.log import _logger
from ..utils.msg import send_bytes_request, ResponseT, RequestT, ResponseStatus


class ServiceProxy:
    """Client proxy for calling services."""

    @staticmethod
    def request(
        service_name: str,
        request: RequestT,
        timeout: float,
    ) -> Optional[ResponseT]:
        """Send a request to a service and get the response."""
        return LanComLoopManager.get_instance().submit_loop_task_and_wait(
            ServiceProxy.request_async(service_name, request, timeout)
        )

    @staticmethod
    async def request_async(
        service_name: str,
        request: RequestT,
        timeout: float,
    ) -> Optional[ResponseT]:
        """Send an asynchronous request to a service and get the response."""
        nodes_manager = NodesInfoManager.get_instance()
        service_info = nodes_manager.get_service_info(service_name)
        if service_info is None:
            _logger.warning("Service %s is not exist", service_name)
            return None
        addr = f"tcp://{service_info['ip']}:{service_info['port']}"
        if isinstance(request, bytes):
            request_bytes = request
        else:
            request_bytes = msgpack.packb(request, use_bin_type=True)
        if request_bytes is None:
            _logger.error("Failed to pack request for service %s", service_name)
            return None
        response = await send_bytes_request(addr, service_name, request_bytes, timeout)
        if response is None:
            return None
        if response[0].decode() != ResponseStatus.SUCCESS:
            _logger.error(
                "Service %s returned error status: %s",
                service_name,
                response[0].decode(),
            )
            return None
        return msgpack.unpackb(response[1])