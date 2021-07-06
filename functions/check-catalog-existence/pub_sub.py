import json
import logging

from google.cloud import pubsub_v1


def publish_to_topic(topic_project_id, topic_name, message, gobits):
    date = ""
    if "received_on" in message:
        date = message["received_on"]
    msg = {"gobits": gobits, "data": message}
    try:
        # Publish to topic
        publisher = pubsub_v1.PublisherClient()
        topic_path = "projects/{}/topics/{}".format(
            topic_project_id, topic_name
        )
        future = publisher.publish(
            topic_path, bytes(json.dumps(msg).encode("utf-8"))
        )
        if date:
            future.add_done_callback(
                lambda x: logging.debug("Published parsed ckan issue")
            )
        future.add_done_callback(
            lambda x: logging.debug("Published parsed ckan issue")
        )
        logging.info("Published parsed ckan issue")
        return True
    except Exception as e:
        logging.exception(
            "Unable to publish parsed ckan issue"
            + "to topic because of {}".format(e)
        )
    return False
