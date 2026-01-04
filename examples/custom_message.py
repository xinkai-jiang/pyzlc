from typing import TypedDict, List

import pyzlc


class CustomMessage(TypedDict):
    """A custom message structure."""
    count: int
    name: str
    data: List[float]


def message_callback(msg: CustomMessage):
    """Callback function to handle received custom messages."""
    # print("Received custom message:", msg)
    pyzlc.info("========== Received Custom Message ==========")
    pyzlc.info("Topic received message %s", msg["count"])
    pyzlc.info("Name: %s", msg["name"])
    pyzlc.info("Values: %s", msg["data"])


if __name__ == "__main__":
    pyzlc.init("CustomMessageNode", "127.0.0.1")
    pyzlc.register_subscriber_handler("CustomMessage", message_callback)
    pyzlc.spin()
