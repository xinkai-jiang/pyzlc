import pyzlc


def create_topic_callback(topic_name: str):
    """Create a topic callback that prints the received message."""

    def topic_callback(msg: str):
        pyzlc.info("Topic %s received message: %s", topic_name, msg)

    return topic_callback


if __name__ == "__main__":
    pyzlc.init("TopicExampleNode", "127.0.0.1")
    publisher = pyzlc.Publisher("example_topic")
    pyzlc.register_subscriber_handler(
        "example_topic", create_topic_callback("example_topic")
    )
    while pyzlc.is_running():
        publisher.publish("Hello, pyzlc!")
        pyzlc.info("Published message to topic 'example_topic'")
        pyzlc.sleep(1)
