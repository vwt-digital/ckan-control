import logging
import json
import base64
import os
from ckanprocessor import CKANProcessor
import requests
from google.cloud import logging as cloud_logging
import datetime
import time

parser = CKANProcessor()

logging.basicConfig(level=logging.INFO)


def request_log(cloud_logger, project_id, function_name):
    # Get timestamp of three minutes ago
    time_stamp = time_format(
        (datetime.datetime.utcnow() - datetime.timedelta(seconds=180)))
    logging.info("Filtering logs on timestamp: {}".format(time_stamp))

    log_filter = "severity = DEBUG " \
                 "AND resource.labels.function_name = \"{}\" " \
                 "AND timestamp >= \"{}\" ".format(function_name, time_stamp)

    # Get all logs with the log filter
    entries = cloud_logger.list_entries(
        filter_=log_filter, order_by=cloud_logging.DESCENDING,
        projects=[project_id])

    return entries


def time_format(dt):
    return "%s:%.3f%sZ" % (
        dt.strftime('%Y-%m-%dT%H:%M'),
        float("%.3f" % (dt.second + dt.microsecond / 1e6)),
        dt.strftime('%z')
    )


def schema_to_ckan(request):
    # Extract data from request
    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    # Extract subscription from subscription string
    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription} [{payload}]')

        # First wait on the execution of the function that uploads data catalogs to CKAN
        cloud_client = cloud_logging.Client()
        log_name = 'cloudfunctions.googleapis.com%2Fcloud-functions'
        cloud_logger = cloud_client.logger(log_name)
        # Function name of the function to wait on
        func_to_wait_on = os.environ.get('FUNC_TO_WAIT_ON', 'Required parameter is missing')
        # Project id where the function is
        # If function is not in the same project ID of where this function is executed,
        # a delegated SA should be added
        project_id = os.environ.get('PROJECT_ID', 'Required parameter is missing')

        start_time = time.time()
        # Could take some time before other function has logged
        while True:
            if time.time() - start_time > 40:
                # No logs were found within time limit
                # Other function has probably not been called
                break
            else:
                logging.info('Refreshing logs...')
                entries = request_log(cloud_logger, project_id, func_to_wait_on)
                entries_list = []
                for entry in entries:
                    entries_list.append(entry.payload)

                for i in range(len(entries_list)):
                    if(i-1 >= 0):
                        if('Function execution started' in entries_list[i]
                                and 'Function execution took' not in entries_list[i-1]):
                            return "The execution of function {} has not yet finished".format(func_to_wait_on), 503
                # If logs have been found
                if entries_list:
                    break

        # Upload schema to CKAN if function has been executed or has not run at all
        ckan_host = os.environ.get('CKAN_SITE_URL', 'Required parameter is missing')
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
    logging.info("Hallo")
