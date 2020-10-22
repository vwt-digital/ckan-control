from google.cloud import storage
import json
import config

import google.auth
from google.auth.transport import requests as gcp_requests
from google.auth import iam
from google.oauth2 import service_account

TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'  # nosec


def check_schema_stg(tag):
    # Get schemas bucket from other project
    external_credentials = request_auth_token()
    storage_client_external = storage.Client(credentials=external_credentials)
    storage_bucket = storage_client_external.get_bucket(config.SCHEMAS_BUCKET)
    blob_name = schema_name_from_tag(tag)
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


def schema_name_from_tag(tag):
    tag = tag.replace('/', '-')
    return tag


def request_auth_token():
    try:
        credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])

        request = gcp_requests.Request()
        credentials.refresh(request)

        signer = iam.Signer(request, credentials, config.DELEGATED_SA)
        creds = service_account.Credentials(
            signer=signer,
            service_account_email=config.DELEGATED_SA,
            token_uri=TOKEN_URI,
            scopes=['https://www.googleapis.com/auth/cloud-platform'],
            subject=config.DELEGATED_SA)
    except Exception:
        raise

    return creds
