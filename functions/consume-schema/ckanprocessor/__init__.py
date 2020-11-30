import config
import os
import logging
import requests
import urllib3
import check_storage

from google.cloud import secretmanager

from ckanapi import RemoteCKAN, NotFound, SearchError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class CKANProcessor(object):

    def __init__(self):
        self.meta = config.SCHEMA_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.project_id = os.environ.get('PROJECT_ID', 'Required parameter is missing').replace('\"', '')
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
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

        self.schema_to_ckan(selector_data)

    def schema_to_ckan(self, schema):
        # If the schema has an id
        if '$id' in schema:
            tag_schema = schema['$id']
            # Get all resources on CKAN that are a topic
            resources = self.host.action.resource_search(query="format:topic")
            resources = resources['results']
            for resource in resources:
                # If the resource has a key 'schema_tag'
                if 'schema_tag' in resource:
                    schema_tag_resource = resource['schema_tag']
                    # If the tag of the processed schema coming from the topic
                    # is the same as one of the resources
                    if schema_tag_resource == tag_schema:
                        # Give that resource a schema
                        self.patch_resource(resource, schema)
                    # Else if it has a schema tag but no schema
                    elif 'schema' not in resource:
                        # Check if the schema can be found in the schemas storage
                        schema_from_storage = check_storage.check_schema_stg(schema_tag_resource)
                        if schema_from_storage:
                            # If it can be found, give the resource a schema
                            self.patch_resource(resource, schema_from_storage)
        else:
            logging.info("The schema from the topic does not have an ID")

    def patch_resource(self, resource, schema):
        # Now patch the resource and give it the new schema
        # Could be that the schema is already there
        # It will be overwritten because the new schema should be the right schema
        try:
            resource_dict = {
                'id': resource['id'],
                'package_id': resource['package_id'],
                'name': resource['name'],
                'url': resource['url'],
                'schema': schema
            }
            self.host.action.resource_patch(**resource_dict)
            logging.info(f"Added schema to resource '{resource['name']}'")
        except NotFound:  # Resource does not exist
            logging.info(f"Resource '{resource['name']}' does not exist")
        except SearchError:
            logging.error(f"SearchError occured while updating resource '{resource['name']}'")
