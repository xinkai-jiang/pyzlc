"""Client proxy for calling services."""
from typing import Optional
import msgpack

from ..nodes.nodes_info_manager import NodesInfoManager
from ..nodes.loop_manager import LanComLoopManager
from ..utils.log import logger
from ..utils.msg import send_bytes_request, Response, Request

class ServiceProxy:
    """Client proxy for calling services."""

    @staticmethod
    def request(
        service_name: str,
        request: Request,
    ) -> Optional[Response]:
        """Send a request to a service and get the response."""
        nodes_manager = NodesInfoManager.get_instance()
        loop_manager = LanComLoopManager.get_instance()
        service_info = nodes_manager.get_service_info(service_name)
        if service_info is None:
            logger.warning("Service %s is not exist", service_name)
            return None
        addr = f"tcp://{service_info['ip']}:{service_info['port']}"
        request_bytes = msgpack.packb(request)
        if request_bytes is None:
            logger.error("Failed to pack request for service %s", service_name)
            return None
        response = loop_manager.submit_loop_task(
            send_bytes_request(addr, service_name, request_bytes),
            True,
        )
        return msgpack.unpackb(response)