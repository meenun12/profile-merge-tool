# https://cloud.google.com/pubsub/docs/publisher#publishing_messages
import json
from typing import List
from .constants import VERSION
from concurrent import futures
from google.cloud import pubsub_v1
from .constants import PUBSUB_PROJECT_ID


publisher_client = pubsub_v1.PublisherClient()


def _get_callback(publish_future, data):
    def callback(publish_future):
        try:
            # Wait up to 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            return "TimeoutError"

    return callback


def _publish_message(topic_path: str, message: str, publish_futures: list, env: str):
    # Encode data
    byte_message = bytes(message, encoding="utf-8")

    # When you publish a message, the client returns a future.
    publish_future = publisher_client.publish(
        topic_path, data=byte_message, env=env, version=VERSION
    )

    # Non-blocking. Publish failures are handled in the callback function.
    publish_future.add_done_callback(_get_callback(publish_future, message))
    publish_futures.append(publish_future)


def publish_messages(request_bodies: List[dict], env: str):
    """Publishes multiple messages to a Pub/Sub topic with an error handler.

    Args:
        request_bodies: each will be published to an associated topic
    """
    publish_futures = []

    # get the topics for each request body and prepare the messages before
    # sending them. In this way, if there are errors, we can catch them
    # before publishing anything

    for data in request_bodies:
        for topic_id, bodies in data.items():
            for body in bodies:
                topic_path = publisher_client.topic_path(PUBSUB_PROJECT_ID, topic_id)
                message = json.dumps(body, indent=4, default=str)
                _publish_message(topic_path, message, publish_futures, env)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)





