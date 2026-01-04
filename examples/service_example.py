from typing import Callable
import traceback

import pyzlc


def register_service_handler_callback(service_name: str) -> Callable[[str], str]:
    """Create a service callback that echoes the received message."""

    def service_callback(msg: str) -> str:
        try:
            pyzlc.info("Service %s received message: %s", service_name, msg)
            return msg
        except Exception as e:
            pyzlc.error("Error in service %s: %s", service_name, e)
            traceback.print_exc()
            raise e

    return service_callback


if __name__ == "__main__":
    pyzlc.init("ServiceNode", "127.0.0.1")
    pyzlc.register_service_handler(
        "echo_service", register_service_handler_callback("echo_service")
    )
    pyzlc.wait_for_service("echo_service")
    pyzlc.call("echo_service", "Hello, world!")
