import json
from google.cloud import pubsub_v1
from config.bq import env, K_SERVICE, PUBSUB_PROJECT_ID

publisher = pubsub_v1.PublisherClient()


def send_pubsub_message(data: dict, topic_id: str, **kwargs) -> None:
    """Publishes a single message to the pub sub topic"""
    topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, topic_id)

    byte_message = bytes(json.dumps(data), encoding="utf-8")

    future = publisher.publish(topic_path, data=byte_message, **kwargs)
    print(future.result())


def notify_slack(from_website: str, to_website: str) -> None:
    """Send input text as a notification on Slack channel #data-services-alerts."""
    payload = {
        "name": f"{env} {K_SERVICE}",
        "text": f"Hey <@UAFHN0VMW>! Merging Tool : Jobs from this {from_website} should go to {to_website}",
        "severity": "SUCCESS",
    }
    topic_id = "data-slack-notifier-prod"
    send_pubsub_message(payload, topic_id)

