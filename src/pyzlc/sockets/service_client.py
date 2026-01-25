"""Client proxy for calling services."""

from typing import Optional

from ..nodes.nodes_info_manager import NodesInfoManager
from ..nodes.loop_manager import LanComLoopManager
from ..utils.log import _logger
from ..utils.msg import ResponseT, RequestT
from ..utils.msg import send_request


async def zlc_request_async(
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
    return await send_request(addr, service_name, request, timeout)


def zlc_request(
    service_name: str,
    request: RequestT,
    timeout: float,
) -> Optional[ResponseT]:
    """Send a request to a service and get the response."""
    return LanComLoopManager.get_instance().submit_loop_task_and_wait(
        zlc_request_async(service_name, request, timeout)
    )
