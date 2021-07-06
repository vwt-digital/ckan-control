import logging
import os

import config
import urllib3
from ckan_processor import CKANProcessor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO)
logging.getLogger("googleapiclient.http").setLevel(logging.ERROR)


def check_catalog_existence(request):
    logging.info("Initialized function")
    if (
            "PROJECT_ID" in os.environ
            and "CKAN_API_KEY_SECRET_ID" in os.environ
            and "CKAN_SITE_URL" in os.environ
            and hasattr(config, "DELEGATED_SA")
    ):
        process_bool = CKANProcessor().process(request)
        if process_bool is False:
            logging.info("Catalog existence check has not run")
        else:
            logging.info("Catalog existence check has run")
    else:
        logging.error("Function has insufficient configuration")


if __name__ == "__main__":
    check_catalog_existence(None)
