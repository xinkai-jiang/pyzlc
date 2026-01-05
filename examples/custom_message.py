from typing import TypedDict, List

import pyzlc


class CustomMessage(TypedDict):
    """A custom message structure."""
    count: int
    name: str
    data: List[float]


def message_callback(msg: CustomMessage):
    """Callback function to handle received custom messages."""
    pyzlc.info("========== Received Custom Message ==========")
    pyzlc.info("Topic received message %s", msg["count"])
    pyzlc.info("Name: %s", msg["name"])
    pyzlc.info("Values: %s", msg["data"])


if __name__ == "__main__":
    pyzlc.init("CustomMessageNode", "127.0.0.1")
    pyzlc.register_subscriber_handler("CustomMessage", message_callback)
    pub = pyzlc.Publisher("CustomMessage")
    for _ in range(10):
        pub.publish(
            CustomMessage(count=42, name="example", data=[1.0, 2.0, 3.0])
        )
        pyzlc.sleep(0.5)
