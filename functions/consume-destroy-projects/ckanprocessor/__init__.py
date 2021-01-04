import os
import logging
import requests

from google.cloud import secretmanager

from ckanapi import RemoteCKAN, NotFound


class CKANProcessor(object):
    def __init__(self):
        self.project_id = os.environ.get('PROJECT_ID', 'Required parameter is missing')
        self.api_key_secret_id = os.environ.get('API_KEY_SECRET_ID', 'Required parameter is missing')
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{self.project_id}/secrets/{self.api_key_secret_id}/versions/latest"
        key_response = client.access_secret_version(request={"name": secret_name})
        self.api_key = key_response.payload.data.decode("UTF-8")
        self.ckan_host = os.environ.get('CKAN_SITE_URL', 'Required parameter is missing')
        self.session = requests.Session()
        self.session.verify = True
        self.host = RemoteCKAN(self.ckan_host, apikey=self.api_key, session=self.session)

    def process(self, payload):
        destroy_projects = payload['destroy_projects']

        for destroy_project in destroy_projects:
            destroy_pid = destroy_project['project_id']
            group = self.get_project_group(destroy_pid)

            if group:
                logging.info(f'Removing datasets of {destroy_pid} from CKAN')
                for package in group.get('packages', []):
                    if package['project_id'] == destroy_pid:
                        self.purge_dataset(package['name'])

                self.host.action.group_purge(id=destroy_pid)

    def get_project_group(self, project_id):
        group = None

        try:
            group = self.host.action.group_show(id=project_id, include_datasets=True)
        except NotFound:  # Group does not exists
            logging.info(f"Group of {project_id} is already deleted")
        except Exception:
            raise

        return group

    def purge_dataset(self, package_name):
        logging.info(f"Purging dataset '{package_name}' and it's resources")

        try:
            package = self.host.action.package_show(id=package_name)  # Retrieve package

            for resource in package.get('resources', []):  # Delete package resources
                self.host.action.resource_delete(id=resource['id'])

            self.host.action.dataset_purge(id=package['id'])  # Purge package
        except NotFound:
            pass
        except Exception:
            raise
