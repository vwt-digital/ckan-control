import os
import logging
import config
import google.auth
import pytz
import datetime
import json
import googleapiclient.discovery

from google.auth import iam
from google.auth.transport import requests as gcp_requests
from google.oauth2 import service_account
from google.cloud import storage, exceptions as gcp_exceptions

from ckanapi import RemoteCKAN, NotFound

TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'  # nosec
logging.basicConfig(level=logging.INFO)


class CKANProcessor(object):
    def __init__(self):
        self.host = RemoteCKAN(os.environ.get('CKAN_SITE_URL'), apikey=os.environ.get('CKAN_API_KEY'))

        self.credentials = request_auth_token()
        self.stg_client = storage.Client(credentials=self.credentials)
        self.su_client = googleapiclient.discovery.build(
            'serviceusage', 'v1', credentials=self.credentials, cache_discovery=False)
        self.sql_client = googleapiclient.discovery.build(
            'sqladmin', 'v1beta4', credentials=self.credentials, cache_discovery=False)

        self.project_services = {}

    def process(self):
        try:
            self.host.action.site_read()
        except Exception:
            logging.error('CKAN not reachable')
        else:
            try:
                package_list = self.host.action.package_list()
            except Exception:
                raise

            logging.info(f"Checking data-catalog existence for {len(package_list)} packages")

            for key in package_list:
                try:
                    package = self.host.action.package_show(id=key)
                except NotFound:
                    logging.error(f"Package with key '{key}' not found")
                    continue
                else:
                    package_name = package.get('name', '').replace('_', '-')

                    if 'project_id' in package:
                        project_id = package['project_id']
                        if project_id not in self.project_services:
                            self.project_services[project_id] = self.get_project_services(project_id)

                        self.Package(
                            package=package,
                            stg_client=self.stg_client,
                            sql_client=self.sql_client,
                            project_services=self.project_services.get(project_id, [])).process()
                    else:
                        logging.debug(f"No Project ID specified for package '{package_name}'")

    def get_project_services(self, project_id):
        response = self.su_client.services().list(parent=f"projects/{project_id}", filter="state:ENABLED").execute()
        return [service.get('config').get('name') for service in response.get('services', [])]

    class Package(object):
        def __init__(self, package, stg_client, sql_client, project_services):
            self.package = package
            self.package_name = package.get('name', '').replace('_', '-')
            self.project_id = package.get('project_id', '')
            self.stg_client = stg_client
            self.sql_client = sql_client
            self.project_services = project_services

        def process(self):
            if self.package.get('resources', None):
                for resource in self.package.get('resources', []):
                    try:
                        if resource['format'] == 'blob-storage':
                            self.check_storage(resource)
                        elif resource['format'] in ['datastore', 'datastore-index']:
                            self.check_service(resource, 'datastore.googleapis.com')
                        elif resource['format'] == 'firestore':
                            self.check_service(resource, 'firestore.googleapis.com')
                        elif resource['format'] in ['cloudsql-instance', 'cloudsql-db']:
                            self.check_cloudsql(resource)
                        else:
                            logging.debug(f"Skipping resource '{resource['name']}' with format '{resource['format']}'")
                            continue
                    except ResourceNotFound as e:
                        logging.error(json.dumps(e.properties))
                        continue
            else:
                logging.error(f"Dataset '{self.package_name}' does not have any resources")

        def check_storage(self, resource):
            try:
                buckets = self.stg_client.list_buckets(project=self.project_id, fields='items/name')
            except (gcp_exceptions.NotFound, gcp_exceptions.Forbidden):
                raise ResourceNotFound(self.package, resource)
            else:
                for bucket in buckets:
                    if bucket.name == resource['name']:
                        return True

                raise ResourceNotFound(self.package, resource)

        def check_cloudsql(self, resource):
            resources = []
            instances_response = self.sql_client.instances().list(project=self.project_id).execute()
            instances = [item['name'] for item in instances_response.get('items', [])]

            if resource['format'] == 'cloudsql-db':
                for instance in instances:
                    databases = self.sql_client.databases().list(project=self.project_id, instance=instance).execute()
                    for database in databases.get('items', []):
                        resources.append(database['name'])
            else:
                resources = instances

            if resource['name'] not in resources:
                raise ResourceNotFound(self.package, resource)

        def check_service(self, resource, service_name):
            if service_name not in self.project_services:
                raise ResourceNotFound(self.package, resource)


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


class ResourceNotFound(Exception):
    def __init__(self, package, resource=None):
        timezone = pytz.timezone("Europe/Amsterdam")
        timestamp = datetime.datetime.now(tz=timezone)

        self.properties = {
            "error": {
                "message": "Resource not found",
                "project_id": package.get('projectId'),
                "package_name": package.get('name').replace('_', '-'),
                "resource_name": resource.get('name', ''),
                "type": resource.get('format', ''),
                "access_url": resource.get('url', ''),
                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }
        }


def check_catalog_existence(request):
    logging.info("Initialized function")

    if 'CKAN_API_KEY' in os.environ and 'CKAN_SITE_URL' in os.environ:
        CKANProcessor().process()
    else:
        logging.error('Function has insufficient configuration')


if __name__ == '__main__':
    check_catalog_existence(None)
