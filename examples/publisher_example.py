from typing import TypedDict
import time

import pyzlc


class TimeStampMessage(TypedDict):
    timestamp: float
    count: int


if __name__ == "__main__":
    pyzlc.init("Publisher", "127.0.0.1")
    pub = pyzlc.Publisher("GreetingTopic")
    count = 0
    try:
        pyzlc.info("Starting publisher...")
        while True:
            pub.publish(TimeStampMessage(timestamp=time.monotonic(), count=count))
            pyzlc.info("Published message with count: %d", count)
            count += 1
            pyzlc.sleep(0.02)
    except KeyboardInterrupt:
        pyzlc.info("Publisher interrupted by user")
        pyzlc.get_node().stop_node()
        raise
