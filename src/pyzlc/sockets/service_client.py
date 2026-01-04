"""Client proxy for calling services."""

from typing import Optional
import msgpack

from ..nodes.nodes_info_manager import NodesInfoManager
from ..nodes.loop_manager import LanComLoopManager
from ..utils.log import _logger
from ..utils.msg import send_bytes_request, ResponseT, RequestT


class ServiceProxy:
    """Client proxy for calling services."""

    @staticmethod
    def request(
        service_name: str,
        request: RequestT,
    ) -> Optional[ResponseT]:
        """Send a request to a service and get the response."""
        nodes_manager = NodesInfoManager.get_instance()
        loop_manager = LanComLoopManager.get_instance()
        service_info = nodes_manager.get_service_info(service_name)
        if service_info is None:
            _logger.warning("Service %s is not exist", service_name)
            return None
        addr = f"tcp://{service_info['ip']}:{service_info['port']}"
        request_bytes = msgpack.packb(request)
        if request_bytes is None:
            _logger.error("Failed to pack request for service %s", service_name)
            return None
        response = loop_manager.submit_loop_task_and_wait(
            send_bytes_request(addr, service_name, request_bytes),
        )
        # TODO: check the status of response
        return msgpack.unpackb(response[1]) if response else None
