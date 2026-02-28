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
    pyzlc.info("Starting publisher...")
    while True:
        pub.publish(TimeStampMessage(timestamp=time.monotonic(), count=count))
        pyzlc.info("Published message with count: %d", count)
        count += 1
        pyzlc.sleep(1)
