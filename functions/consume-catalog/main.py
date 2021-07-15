import base64
import json
import logging
import os

import requests
from ckan_processor import CKANProcessor

parser = CKANProcessor()

logging.basicConfig(level=logging.INFO)


# First json to postgis, then postgis to database
def json_to_ckan(request):
    # Extract data from request
    envelope = json.loads(request.data.decode("utf-8"))
    payload = base64.b64decode(envelope["message"]["data"])

    # Extract subscription from subscription string
    try:
        subscription = envelope["subscription"].split("/")[-1]
        logging.info(f"Message received from {subscription} [{payload}]")

        ckan_host = os.environ.get("CKAN_SITE_URL", "Required parameter is missing")
        status = requests.head(ckan_host).status_code
        if status == 200:
            parser.process(json.loads(payload))
        else:
            logging.info("CKAN is down")
            return "CKAN down", 503

    except Exception as e:
        logging.info("Extract of subscription failed")
        logging.debug(e)
        raise e

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successful, no further actions needed
    return "OK", 204
