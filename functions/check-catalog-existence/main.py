import os
import logging
import config
import google.auth
import pytz
import datetime
import requests
import sys
import signal
import threading
import googleapiclient.discovery
import atlassian

from google.auth import iam
from google.auth.transport import requests as gcp_requests
from google.oauth2 import service_account
from google.cloud import secretmanager, storage, bigquery

from ckanapi import RemoteCKAN, NotFound
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'  # nosec

logging.basicConfig(level=logging.INFO)
logging.getLogger("googleapiclient.http").setLevel(logging.ERROR)


class CKANProcessor(object):
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = True

        self.ckan_host = os.environ.get('CKAN_SITE_URL')
        self.ckan_api_key_secret_id = os.environ.get('CKAN_API_KEY_SECRET_ID')
        self.project_id = os.environ.get('PROJECT_ID')

        self.secret_client = secretmanager.SecretManagerServiceClient()
        ckan_api_key_secret = self.secret_client.access_secret_version(
            request={"name": f"projects/{self.project_id}/secrets/{self.ckan_api_key_secret_id}/versions/latest"})
        self.ckan_api_key = ckan_api_key_secret.payload.data.decode("UTF-8")

        self.host = RemoteCKAN(self.ckan_host, apikey=self.ckan_api_key, session=self.session)

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
        status = requests.head(self.ckan_host, verify=True).status_code
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
                    pass
                    logging.error(f"Package with key '{key}' not found")
                    continue
                else:
                    package_name = package.get('name', '').replace('_', '-')

                    if 'project_id' in package:
                        project_id = package['project_id']
                        if project_id not in self.project_services:
                            self.project_services[project_id] = self.get_project_services(project_id)

                        if not self.project_services[project_id]:
                            timezone = pytz.timezone("Europe/Amsterdam")
                            timestamp = datetime.datetime.now(tz=timezone)

                            not_found_resources.append({
                                "message": "Project not found",
                                "project_id": project_id,
                                "package_name": "google-cloud-project",
                                "resource_name": project_id,
                                "type": "GCP Project",
                                "access_url": f"https://console.cloud.google.com/home/dashboard?project={project_id}",
                                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                            })

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
                JiraProcessor(self.secret_client).create_issues(not_found_resources)
        else:
            logging.error('CKAN not reachable')

    def get_project_services(self, project_id):
        try:
            response = self.su_client.services().list(
                parent=f"projects/{project_id}", filter="state:ENABLED").execute()
            return [service.get('config').get('name') for service in response.get('services', [])]
        except Exception:
            pass
            return None

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
                                logging.debug(
                                    f"Skipping resource '{resource['name']}' with format '{resource['format']}'")
                                continue
                        except ResourceNotFound as e:
                            pass
                            self.not_found_resources.append(e.properties['error'])
                            continue
                        except TimeOutException:
                            pass
                            logging.info(
                                f"A timeout occurred when checking resource '{resource['name']}' " +
                                f"with format '{resource['format']}': the request took more than 20s")
                            continue
                        except Exception as e:
                            pass
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

                for bucket in buckets:
                    if bucket.name == resource['name']:
                        return True
            except Exception:
                pass
            raise ResourceNotFound(self.package, resource)

        def check_cloudsql(self, resource):
            try:
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
            except Exception:
                pass
                raise ResourceNotFound(self.package, resource)
            else:
                if resource['name'] not in resources:
                    raise ResourceNotFound(self.package, resource)

        def check_bigquery(self, resource):
            try:
                datasets_list = self.bq_client.list_datasets(project=self.project_id)
                datasets = [dataset.dataset_id for dataset in datasets_list]
            except Exception:
                pass
                raise ResourceNotFound(self.package, resource)
            else:
                if resource['name'] not in datasets:
                    raise ResourceNotFound(self.package, resource)

        def check_pubsub(self, resource):
            try:
                if resource['format'] == 'subscription':
                    subscriptions = self.ps_client.projects().subscriptions().list(project=f"projects/{self.project_id}").execute()
                    resources = [sub['name'].split('/')[-1] for sub in subscriptions.get('subscriptions', [])]
                else:
                    topics = self.ps_client.projects().topics().list(project=f"projects/{self.project_id}").execute()
                    resources = [top['name'].split('/')[-1] for top in topics.get('topics', [])]
            except Exception:
                pass
                raise ResourceNotFound(self.package, resource)
            else:
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


class JiraProcessor(object):
    def __init__(self, secret_client):
        self.secret_client = secret_client

    def create_issues(self, not_found_resources):
        # Checking for JIRA attributes in configuration file
        for item in ['JIRA_ACTIVE', 'JIRA_USER', 'JIRA_SERVER', 'JIRA_PROJECT', 'JIRA_PROJECTS', 'JIRA_BOARD',
                     'JIRA_EPIC', 'JIRA_SECRET_ID']:
            if not hasattr(config, item):
                logging.error('Function has insufficient configuration for creating JIRA issues')
                sys.exit(1)

        if not config.JIRA_ACTIVE:
            not_found_resources_names = [resource["resource_name"] for resource in not_found_resources]
            logging.error(
                f"Creating JIRA tickets is manually disabled. Check processed a total of {len(not_found_resources)} "
                f"missing resources: {', '.join(not_found_resources_names)}")
            return None

        # Setup configuration variables
        gcp_project_id = os.environ.get('PROJECT_ID')
        jira_user = config.JIRA_USER
        jira_server = config.JIRA_SERVER
        jira_project = config.JIRA_PROJECT
        jira_projects = config.JIRA_PROJECTS
        jira_board = config.JIRA_BOARD
        jira_epic = config.JIRA_EPIC
        jira_secret_id = config.JIRA_SECRET_ID

        jira_api_key_secret = self.secret_client.access_secret_version(
            request={"name": f"projects/{gcp_project_id}/secrets/{jira_secret_id}/versions/latest"})
        jira_api_key = jira_api_key_secret.payload.data.decode("UTF-8")

        # Create Jira instance
        client = atlassian.jira_init(jira_user, jira_api_key, jira_server)

        # Create Jira filters
        jql_prefix = f"type = Bug AND status != Done AND status != Cancelled " \
                     f"AND \"Epic Link\" = {jira_epic} " \
                     "AND text ~ \"CKAN resource not found\" " \
                     "AND project = "
        projects = [jql_prefix + project for project in jira_projects.split('+')]
        jql = " OR ".join(projects)

        # Retrieve current issues and sprint ID
        titles = atlassian.list_issue_titles(client, jql)
        sprint_id = atlassian.get_current_sprint(client, jira_board)

        # Create JIRA issue objects for non-existing packages that don't have a JIRA ticket already
        for resource in not_found_resources:
            title = f"CKAN resource not found: '{resource['resource_name']}'"

            if title not in titles:
                logging.info(f"Creating jira ticket: \"{title}\"")
                if resource['package_name'] == "google-cloud-project":
                    description = (
                        f"The Google Cloud Project `{resource['project_id']}` could not be found. Please check the "
                        "existence of the project within GCP, or remove the dataset from the data-catalog.")
                else:
                    description = (
                        f"The resource `{resource['project_id']}/{resource['package_name']}/"
                        f"{resource['resource_name']}` could not be identified by the automated data-catalog existence "
                        f"check. Please check the existence of the resource within GCP, or remove the dataset resource "
                        f"from the data-catalog.")

                issue = atlassian.create_issue(
                    client=client,
                    project=jira_project,
                    title=title,
                    description=description)

                atlassian.add_to_epic(client, jira_epic, issue.key)
                atlassian.add_to_sprint(client, sprint_id, issue.key)


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
                "project_id": package.get('project_id'),
                "package_name": package.get('name').replace('_', '-'),
                "resource_name": resource.get('name', ''),
                "type": resource.get('format', ''),
                "access_url": resource.get('url', ''),
                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }
        }


def check_catalog_existence(request):
    logging.info("Initialized function")

    if 'PROJECT_ID' in os.environ and \
       'CKAN_API_KEY_SECRET_ID' in os.environ and \
       'CKAN_SITE_URL' in os.environ and hasattr(config, 'DELEGATED_SA'):
        CKANProcessor().process()
    else:
        logging.error('Function has insufficient configuration')


class TimeOutException(Exception):
    pass


def alarm_handler(signum, frame):
    print("ALARM signal received")
    raise TimeOutException()


if threading.current_thread() == threading.main_thread():
    signal.signal(signal.SIGALRM, alarm_handler)


if __name__ == '__main__':
    check_catalog_existence(None)
