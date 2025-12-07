from typing import TypedDict

from pyzerolancom import init_node
from pyzerolancom.utils.log import logger

class CustomMessage(TypedDict):
    """A custom message structure."""
    count: int
    name: str
    data: list[float]

def message_callback(msg: CustomMessage):
    """Callback function to handle received custom messages."""
    # print("Received custom message:", msg)
    logger.info("========== Received Custom Message ==========")
    logger.info("Topic received message %s", msg["count"])
    logger.info("Name: %s", msg["name"])
    logger.info("Values: %s", msg["data"])

if __name__ == "__main__":
    node = init_node("CustomMessageNode", "127.0.0.1")
    node.create_subscriber("CustomMessage", message_callback)
    node.spin()
