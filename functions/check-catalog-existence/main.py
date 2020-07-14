import os
import logging
import config
import google.auth
import pytz
import datetime
import requests
import json
import sys
import signal
import re
import googleapiclient.discovery

from requests.auth import HTTPBasicAuth
from google.auth import iam
from google.auth.transport import requests as gcp_requests
from google.oauth2 import service_account
from google.cloud import storage, bigquery, exceptions as gcp_exceptions

from ckanapi import RemoteCKAN, NotFound

TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'  # nosec
logging.basicConfig(level=logging.INFO)


class CKANProcessor(object):
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.host = RemoteCKAN(os.environ.get('CKAN_SITE_URL'), apikey=os.environ.get('CKAN_API_KEY'), session=self.session)

        self.credentials = request_auth_token()
        self.stg_client = storage.Client(credentials=self.credentials)
        self.bq_client = bigquery.Client(credentials=self.credentials)
        self.ps_client = googleapiclient.discovery.build(
            'pubsub', 'v1', credentials=self.credentials, cache_discovery=False)
        self.su_client = googleapiclient.discovery.build(
            'serviceusage', 'v1', credentials=self.credentials, cache_discovery=False)
        self.sql_client = googleapiclient.discovery.build(
            'sqladmin', 'v1beta4', credentials=self.credentials, cache_discovery=False)

        self.project_services = {}

    def process(self):
        ckan_host = os.environ.get('CKAN_SITE_URL', 'Required parameter is missing')
        status = requests.head(ckan_host, verify=False).status_code
        if status == 200:
            try:
                package_list = self.host.action.package_list()
            except Exception:
                raise

            logging.info(f"Checking data-catalog existence for {len(package_list)} packages")
            not_found_resources = []

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

                        not_found_resources.extend(self.Package(
                            package=package,
                            stg_client=self.stg_client,
                            bq_client=self.bq_client,
                            ps_client=self.ps_client,
                            sql_client=self.sql_client,
                            project_services=self.project_services.get(project_id, [])).process())
                    else:
                        logging.debug(f"No Project ID specified for package '{package_name}'")

            if len(not_found_resources) > 0:
                create_jira_issues(not_found_resources)
        else:
            logging.error('CKAN not reachable')

    def get_project_services(self, project_id):
        response = self.su_client.services().list(parent=f"projects/{project_id}", filter="state:ENABLED").execute()
        return [service.get('config').get('name') for service in response.get('services', [])]

    class Package(object):
        def __init__(self, package, stg_client, bq_client, ps_client, sql_client, project_services):
            self.package = package
            self.package_name = package.get('name', '').replace('_', '-')
            self.project_id = package.get('project_id', '')
            self.project_services = project_services

            self.stg_client = stg_client
            self.bq_client = bq_client
            self.ps_client = ps_client
            self.sql_client = sql_client

            self.not_found_resources = []

        def process(self):
            if self.package.get('resources', None):
                signal.signal(signal.SIGALRM, alarm_handler)

                for resource in self.package.get('resources', []):
                    if 'format' in resource and 'name' in resource:
                        signal.alarm(20)

                        try:
                            if resource['format'] == 'blob-storage':
                                self.check_storage(resource)
                            elif resource['format'] in ['datastore', 'datastore-index']:
                                self.check_service(resource, 'datastore.googleapis.com')
                            elif resource['format'] == 'firestore':
                                self.check_service(resource, 'firestore.googleapis.com')
                            elif resource['format'] in ['cloudsql-instance', 'cloudsql-db']:
                                self.check_cloudsql(resource)
                            elif resource['format'] == 'bigquery-dataset':
                                self.check_bigquery(resource)
                            elif resource['format'] in ['topic', 'subscription']:
                                self.check_pubsub(resource)
                            elif resource['format'] == 'API':
                                self.check_api(resource)
                            else:
                                logging.debug(f"Skipping resource '{resource['name']}' with format '{resource['format']}'")
                                continue
                        except ResourceNotFound as e:
                            self.not_found_resources.append(e.properties['error'])
                            continue
                        except TimeOutException:
                            logging.info(
                                f"A timeout occurred when checking resource '{resource['name']}' " +
                                f"with format '{resource['format']}': the request took more than 20s")
                            continue
                        except Exception as e:
                            logging.info(
                                f"An exception occurred when checking resource '{resource['name']}' " +
                                f"with format '{resource['format']}': {str(e)}")
                            continue
                        else:
                            signal.alarm(0)
                    else:
                        logging.info(f"Resource does not have the correct fields: {resource}")
            else:
                logging.info(f"Dataset '{self.package_name}' does not have any resources")

            return self.not_found_resources

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

        def check_bigquery(self, resource):
            datasets_list = self.bq_client.list_datasets(project=self.project_id)
            datasets = [dataset.dataset_id for dataset in datasets_list]

            if resource['name'] not in datasets:
                raise ResourceNotFound(self.package, resource)

        def check_pubsub(self, resource):
            if resource['format'] == 'subscription':
                subscriptions = self.ps_client.projects().subscriptions().list(project=f"projects/{self.project_id}").execute()
                resources = [sub['name'].split('/')[-1] for sub in subscriptions.get('subscriptions', [])]
            else:
                topics = self.ps_client.projects().topics().list(project=f"projects/{self.project_id}").execute()
                resources = [top['name'].split('/')[-1] for top in topics.get('topics', [])]

            if resource['name'] not in resources:
                raise ResourceNotFound(self.package, resource)

        def check_api(self, resource):
            if 'url' in resource:
                response = requests.get(resource['url'])

                if not response.ok:
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


def create_jira_issues(not_found_resources):
    # Checking for JIRA attributes in configuration file
    for item in ['JIRA_ACTIVE', 'JIRA_USER', 'JIRA_API_DOMAIN', 'JIRA_PROJECT_ID', 'JIRA_ISSUE_TYPE_ID',
                 'JIRA_BOARD_ID', 'JIRA_EPIC']:
        if not hasattr(config, item):
            logging.error('Function has insufficient configuration for creating JIRA issues')
            sys.exit(1)

    # Checking for the JIRA API key
    if 'JIRA_API_KEY' not in os.environ:
        logging.error('Function has insufficient environment variables for creating JIRA issues')
        sys.exit(1)

    # Check if JIRA functionality is active
    if not config.JIRA_ACTIVE:
        logging.info(f"JIRA is inactive, processed a total of {len(not_found_resources)} missing resources")
        logging.info(json.dumps(not_found_resources))
        return

    # Setup JIRA request variables
    headers = {"content-type": "application/json"}
    auth = HTTPBasicAuth(config.JIRA_USER, os.environ['JIRA_API_KEY'])
    issues_already_existing = {}
    issues_to_create = []

    # Search for already existing JIRA tickets with format "CKAN resource not found: '<dataset_name>'"
    issues_search_payload = {"jql": (f"project = {config.JIRA_PROJECT_ID} "
                                     f"AND issuetype = {config.JIRA_ISSUE_TYPE_ID} "
                                     "AND status in (\"To Do\", \"In Progress\", \"For Review\", Feedback) "
                                     f"AND \"Epic Link\" = {config.JIRA_EPIC} "
                                     "AND text ~ \"CKAN resource not found\" "
                                     "ORDER BY priority DESC"),
                             "fields": ["summary"]}
    issues_search_response = requests.post(f"{config.JIRA_API_DOMAIN}/rest/api/3/search", headers=headers, auth=auth,
                                           data=json.dumps(issues_search_payload))

    if issues_search_response.ok:
        issues_search_content = json.loads(issues_search_response.content)

        for issue in issues_search_content.get("issues", []):
            re_match = re.match(r"(?:CKAN resource not found: )(?:')([\w-]+)(?:')", issue['fields']['summary'])
            if re_match:
                issues_already_existing[re_match.group(1)] = issue['key']

    # Create JIRA issue objects for non-existing packages that don't have a JIRA ticket already
    for resource in not_found_resources:
        if resource['resource_name'] not in issues_already_existing:
            issues_to_create.append({
                "fields": {
                    "project": {"id": int(config.JIRA_PROJECT_ID)},
                    "issuetype": {"id": int(config.JIRA_ISSUE_TYPE_ID)},
                    "customfield_10014": config.JIRA_EPIC,
                    "summary": "CKAN resource not found: '{}'".format(resource['resource_name']),
                    "description": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {
                                "type": "paragraph",
                                "content": [
                                    {
                                        "text": (
                                            "The resource `{}/{}/{}` could not be identified by the automated "
                                            "data-catalog existence check. Please check the existence of the resource "
                                            "within GCP, or remove the dataset resource from the data-catalog.".format(
                                                resource['project_id'], resource['package_name'], resource['resource_name'])),
                                        "type": "text"
                                    }
                                ]
                            }
                        ]
                    }
                }
            })
        else:
            logging.info(
                f"JIRA ticket for issue '{resource['resource_name']}' does already " +
                f"exist: '{issues_already_existing[resource['resource_name']]}'")

    # Post the non-existing packages to JIRA as issues
    if len(issues_to_create) > 0:
        issues_payload = {"issueUpdates": issues_to_create}
        issues_response = requests.post(f"{config.JIRA_API_DOMAIN}/rest/api/3/issue/bulk", headers=headers, auth=auth,
                                        data=json.dumps(issues_payload))

        # Get the current active JIRA spring
        if issues_response.ok:
            logging.info(f"Created {len(issues_to_create)} issues within Epic '{config.JIRA_EPIC}'")
            issues_content = json.loads(issues_response.content)
            new_issues = [issue['id'] for issue in issues_content['issues']]

            sprint_response = requests.get(f"{config.JIRA_API_DOMAIN}/rest/agile/1.0/board/{config.JIRA_BOARD_ID}/sprint",
                                           headers=headers, auth=auth, params={'state': 'active', 'maxResults': 1})

            # Move all created issues towards the current JIRA spring
            if sprint_response.ok:
                sprint_obj = json.loads(sprint_response.content)
                sprint_id = sprint_obj['values'][0]['id'] if len(sprint_obj['values']) > 0 else None

                sprint_payload = {"issues": new_issues}
                sprint_move_response = requests.post(f"{config.JIRA_API_DOMAIN}/rest/agile/1.0/sprint/{sprint_id}/issue",
                                                     headers=headers, auth=auth, data=json.dumps(sprint_payload))

                if sprint_move_response.ok:
                    logging.info(f"Moved {len(new_issues)} issues to Sprint '{sprint_id}'")
                else:
                    logging.error(f"Moving new JIRA issues '{','.join(new_issues)}' returned status " +
                                  f"code '{sprint_move_response.status_code}'")
            else:
                logging.error(f"Retrieving current JIRA sprint returned status code '{sprint_response.status_code}'")
        else:
            logging.error(f"Creating JIRA issues returned status code '{issues_response.status_code}'")


class ResourceNotFound(Exception):
    def __init__(self, package, resource=None):
        timezone = pytz.timezone("Europe/Amsterdam")
        timestamp = datetime.datetime.now(tz=timezone)

        self.properties = {
            "error": {
                "message": "Resource not found",
                "project_id": package.get('project_id'),
                "package_name": package.get('name').replace('_', '-'),
                "resource_name": resource.get('name', ''),
                "type": resource.get('format', ''),
                "access_url": resource.get('url', ''),
                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }
        }


class TimeOutException(Exception):
    pass


def alarm_handler(signum, frame):
    print("ALARM signal received")
    raise TimeOutException()


def check_catalog_existence(request):
    logging.info("Initialized function")

    if 'CKAN_API_KEY' in os.environ and 'CKAN_SITE_URL' in os.environ and hasattr(config, 'DELEGATED_SA'):
        CKANProcessor().process()
    else:
        logging.error('Function has insufficient configuration')


if __name__ == '__main__':
    check_catalog_existence(None)
