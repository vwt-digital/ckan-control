import config
import os
import logging
import requests
import urllib3
import ast
import json

from ckanapi import RemoteCKAN, NotFound, SearchError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class CKANProcessor(object):

    def __init__(self):
        self.meta = config.SCHEMA_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.api_key = os.environ.get('API_KEY', 'Required parameter is missing')
        self.ckan_host = os.environ.get('CKAN_SITE_URL', 'Required parameter is missing')
        self.session = requests.Session()
        self.session.verify = True
        self.host = RemoteCKAN(self.ckan_host, apikey=self.api_key, session=self.session)

    def process(self, payload):
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

        self.schema_to_ckan(selector_data)

    def schema_to_ckan(self, schema):
        schema_json = json.dumps(schema)
        schema_json = json.loads(schema_json)

        logging.info("schema:")
        logging.info(schema)
        logging.info("schema_json:")
        logging.info(schema_json)

        schema_list = [schema]

        # If the schema has an id
        if '$id' in schema_json:
            urn_schema = schema_json['$id']
            # Get all resources on CKAN that are a topic
            resources = self.host.action.resource_search(query="format:topic")
            resources = resources['results']
            for resource in resources:
                # If the resource has a key 'schema_urns'
                if 'schema_urns' in resource:
                    schema_urns_resource = resource['schema_urns']
                    # The value of the key 'schema_urns' is a string that is a list
                    # Convert it to an actual list
                    schema_urns_resource = ast.literal_eval(schema_urns_resource)
                    # If the urn of the processed schema coming from the topic
                    # is the same as one of the resources
                    for urn in schema_urns_resource:
                        if urn == urn_schema:
                            # Give that resource a schema
                            self.patch_resource(resource, schema_list)
        else:
            logging.info("The schema from the topic does not have an ID")

    def patch_resource(self, resource, schema_list):
        not_add_again = []
        # Loop through schema list and check if a schema is already in the resource
        if 'schemas' in resource:
            current_schema_list = resource['schemas']
            current_schema_list = ast.literal_eval(current_schema_list)
            for schema in schema_list:
                schema = json.loads(schema)
                for cs in current_schema_list:
                    cs = json.loads(cs)
                    if '$id' in cs:
                        cs_urn = cs['$id']
                        if cs_urn == schema['$id']:
                            not_add_again.append(cs_urn)
            # If it is, do not add it double to the resource
            for cs in current_schema_list:
                cs = json.loads(cs)
                if '$id' in cs:
                    cs_urn = cs['$id']
                    if cs_urn not in not_add_again:
                        schema_list.append(cs)
        # Now patch the resource and give it the new schema
        logging.info("schema_list:")
        logging.info(schema_list)
        try:
            resource_dict = {
                'id': resource['id'],
                'package_id': resource['package_id'],
                'name': resource['name'],
                'url': resource['url'],
                'schemas': schema_list
            }
            self.host.action.resource_patch(**resource_dict)
            logging.info(f"Added schema to resource '{resource['name']}'")
        except NotFound:  # Resource does not exist
            logging.info(f"Resource '{resource['name']}' does not exist")
        except SearchError:
            logging.error(f"SearchError occured while updating resource '{resource['name']}'")
