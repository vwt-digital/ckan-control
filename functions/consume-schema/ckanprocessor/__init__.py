import config
import os
import logging
import requests
import urllib3
import check_storage
import json

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
        schemas = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

        # Get all resources on CKAN that are a topic
        resources = self.host.action.resource_search(query="format:topic")
        resources = resources['results']

        for schema in schemas:
            references = self.get_refs(schema, schemas)
            self.schema_to_ckan(schema, references, resources)

        # Check if every resource now has schemas
        resources = self.host.action.resource_search(query="format:topic")
        resources = resources['results']

    def get_refs(self, schema, schemas):
        # Find references in schema
        schema_references = self.get_refs_from_schema(schema)
        references = []
        references_not_found = []
        for ref in schema_references:
            references_not_found.append(ref)
        for ref in schema_references:
            # Check if schema references were send in message
            for s in schemas:
                if "$id" in s:
                    if s["$id"] == ref:
                        references_not_found.remove(ref)
                        references.append(json.dumps(s, indent=2))
        # Check if there are schema references not found in message
        references_not_found_two = []
        if len(references_not_found) != 0:
            for r in references_not_found:
                references_not_found_two.append(r)
            for r in references_not_found:
                # Check if the schema can be found in the schemas storage
                schema_from_storage = check_storage.check_schema_stg(r)
                if schema_from_storage:
                    # If it can be found, add it to references
                    references.append(json.dumps(schema_from_storage, indent=2))
                    references_not_found_two.remove(r)
        # Check if all references are found
        if len(references_not_found_two) != 0:
            if "$id" in schema:
                logging.info(f"Schema {schema['$id']} contains references {references_not_found_two}"
                             " that could not be found")
        return references

    def get_refs_from_schema(self, schema):
        references = []
        schema = json.dumps(schema, indent=2)
        # Make schema into list so that every newline can be printed
        schema_list = schema.split('\n')
        for line in schema_list:
            if '$ref' in line:
                # If a '#' is in the reference, it's a reference to a definition
                if '#' in line:
                    if '"$ref": "' in line:
                        line_array_def = line.split('"$ref": "')
                    elif '"$ref" : "' in line:
                        line_array_def = line.split('"$ref" : "')
                    else:
                        line_array_def = ''
                    if line_array_def:
                        ref_def = line_array_def[1].replace('\"', '')
                        # If the reference is not '#'
                        if ref_def != '#':
                            # Check if there is a URI in front of the '#'
                            # Because then the definition is in another schema
                            if 'tag' in ref_def or 'http' in ref_def:
                                # Split on the '#/'
                                ref_def_array = ref_def.split("#/")
                                uri_part = ref_def_array[0]
                                references.append(uri_part)
                # If reference is an URL
                else:
                    if '"$ref": "' in line:
                        line_array = line.split('"$ref": "')
                    elif '"$ref" : "' in line:
                        line_array = line.split('"$ref" : "')
                    else:
                        line_array = ''
                    if line_array:
                        ref = line_array[1].replace('\"', '')
                        references.append(ref)
        return references

    def schema_to_ckan(self, schema, references, resources):
        # If the schema has an id
        if '$id' in schema:
            tag_schema = schema['$id']
            for resource in resources:
                # If the resource has a key 'schema_tag'
                if 'schema_tag' in resource:
                    schema_tag_resource = resource['schema_tag']
                    # If the tag of the processed schema coming from the topic
                    # is the same as one of the resources
                    schemas_to_patch = []
                    if schema_tag_resource == tag_schema:
                        # Give that resource a schema with its references
                        schemas_to_patch = [json.dumps(schema, indent=2)]
                        schemas_to_patch.extend(references)
                        self.patch_resource(resource, schemas_to_patch)
        else:
            logging.info("The schema from the topic does not have an ID")

    def check_resources(self, schemas, resources):
        for resource in resources:
            # If the resource has a key 'schema_tag'
            if 'schema_tag' in resource:
                schema_tag_resource = resource['schema_tag']
                schemas_to_patch = []
                if 'schemas' not in resource:
                    # Check if the schema can be found in the schemas storage
                    schema_from_storage = check_storage.check_schema_stg(schema_tag_resource)
                    if schema_from_storage:
                        # Get the references
                        references_gcp = self.get_refs(schema_from_storage, schemas)
                        # If the schema can be found, give the resource a schema with its references
                        schemas_to_patch = [schema_from_storage]
                        schemas_to_patch.extend(references_gcp)
                        self.patch_resource(resource, schemas_to_patch)

    def patch_resource(self, resource, schemas):
        # Now patch the resource and give it the new schema
        # Could be that the schema is already there
        # It will be overwritten because the new schema should be the right schema
        try:
            resource_dict = {
                'id': resource['id'],
                'package_id': resource['package_id'],
                'name': resource['name'],
                'url': resource['url'],
                'schemas': schemas
            }
            self.host.action.resource_patch(**resource_dict)
            logging.info(f"Added schema to resource '{resource['name']}'")
        except NotFound:  # Resource does not exist
            logging.info(f"Resource '{resource['name']}' does not exist")
        except SearchError:
            logging.error(f"SearchError occured while updating resource '{resource['name']}'")
