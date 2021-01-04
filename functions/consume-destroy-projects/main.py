import logging
import json
import base64
import os
from ckanprocessor import CKANProcessor
import requests

parser = CKANProcessor()

logging.basicConfig(level=logging.INFO)


# First json to postgis, then postgis to database
def json_to_ckan(request):
    # Extract data from request
    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    # Extract subscription from subscription string
    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription} [{payload}]')

        ckan_host = os.environ.get('CKAN_SITE_URL')

        if not ckan_host:
            logging.error('CKAN_SITE_URL should be specified in environment')
            return 'Server error', 500

        status = requests.head(ckan_host).status_code
        if status == 200:
            parser.process(json.loads(payload))
        else:
            logging.info("CKAN is down")
            return 'CKAN down', 503

    except Exception as e:
        logging.info('Extract of subscription failed')
        logging.debug(e)
        raise e

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successfull, no further actions needed
    return 'OK', 204


if __name__ == '__main__':
    logging.info("Testing consume-destroy-projects")
    msg = {
        'destroy_projects': [
            {
                'project_id': 'my-destroy-project'
            }
        ]
    }

    parser.process(msg)
