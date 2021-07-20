import json

import config
from gcp_helper import GCPHelper
from google.cloud import storage


class GCPService:

    def __init__(self):
        self.gcp_helper = GCPHelper()

    def check_schema_stg(self, tag):
        # Get schemas bucket from other project
        external_credentials = self.gcp_helper.request_auth_token()
        storage_client_external = storage.Client(credentials=external_credentials)
        storage_bucket = storage_client_external.get_bucket(config.SCHEMAS_BUCKET)
        # Get schema name from tag
        tag = tag.replace('/', '_')
        if not tag.endswith(".json"):
            tag = tag + ".json"
        blob_name = tag
        # Check if schema is in schema storage
        if storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client_external):
            # Get blob
            blob = storage_bucket.get_blob(blob_name)
            # Convert to string
            blob_json_string = blob.download_as_string()
            # Convert to json
            blob_json = json.loads(blob_json_string)
            # return blob in json format
            return blob_json
        return None
