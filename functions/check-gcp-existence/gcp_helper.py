import json
import logging

import config
import google.auth
from google.auth import iam
from google.auth.transport import requests as gcp_requests
from google.cloud import pubsub_v1
from google.oauth2 import service_account


class GCPHelper:

    def request_auth_token(self):
        try:
            credentials, project_id = google.auth.default(
                scopes=["https://www.googleapis.com/auth/iam"]
            )

            request = gcp_requests.Request()
            credentials.refresh(request)

            signer = iam.Signer(request, credentials, config.DELEGATED_SA)
            token_url = "https://accounts.google.com/o/oauth2/token"  # nosec
            creds = service_account.Credentials(
                signer=signer,
                service_account_email=config.DELEGATED_SA,
                token_uri=token_url,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
                subject=config.DELEGATED_SA,
            )
        except Exception:
            raise

        return creds

    def publish_to_topic(self, topic_project_id, topic_name, messages, gobits):
        if not hasattr(messages, "__len__"):
            messages = [messages]
        for message in messages:
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
            except Exception as e:
                logging.exception(
                    "Unable to publish parsed ckan issue"
                    + "to topic because of {}".format(e)
                )
            return False
        return True
