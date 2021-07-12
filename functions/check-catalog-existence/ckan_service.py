import logging
import os

import requests
from ckanapi import NotFound, RemoteCKAN
from google.cloud import secretmanager


class CKANService:
    def __init__(self):
        self.project_id = os.environ.get("PROJECT_ID")
        self.ckan_host = os.environ.get("CKAN_SITE_URL")
        self.ckan_api_key_secret_id = os.environ.get("CKAN_API_KEY_SECRET_ID")
        self.secret_client = secretmanager.SecretManagerServiceClient()
        ckan_api_key_secret = self.secret_client.access_secret_version(
            request={
                "name": f"projects/{self.project_id}/secrets/{self.ckan_api_key_secret_id}/versions/latest"
            }
        )
        self.ckan_api_key = ckan_api_key_secret.payload.data.decode("UTF-8")
        self.session = requests.Session()
        self.session.verify = True
        self.host = RemoteCKAN(
            self.ckan_host, apikey=self.ckan_api_key, session=self.session
        )

    def is_ckan_reachable(self):
        status = requests.head(self.ckan_host, verify=True).status_code
        if status != 200:
            logging.error("CKAN not reachable")
            return False
        return True

    def get_project_group(self, group_project_id):
        group = None
        try:
            group = self.host.action.group_show(
                id=group_project_id, include_datasets=True
            )
        except NotFound:  # Group does not exists
            logging.info(f"Group of {group_project_id} could not be found")
        except Exception:
            raise
        return group

    def get_group_list(self):
        # Get all groups of CKAN, they are based on GCP project IDs
        return self.host.action.group_list()

    def get_full_package(self, package_id):
        return self.host.action.package_show(id=package_id)
