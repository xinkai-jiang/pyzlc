import pyzlc
import time

from publisher_example import TimeStampMessage

def message_callback(msg: TimeStampMessage):
    delay = (time.monotonic() - msg["timestamp"]) * 1000
    pyzlc.info("Received message: delay=%f ms, count=%d", delay, msg["count"])

if __name__ == "__main__":
    pyzlc.init("Subscriber", "127.0.0.1")
    pyzlc.register_subscriber_handler("GreetingTopic", message_callback)
    try:
        pyzlc.info("Subscriber is running. Press Ctrl+C to exit.")
        pyzlc.spin()
    except KeyboardInterrupt:
        pyzlc.info("Subscriber interrupted by user")
        pyzlc.get_node().stop_node()
        raise