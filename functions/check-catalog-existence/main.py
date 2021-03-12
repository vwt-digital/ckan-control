import os
import logging
import config
import google.auth
import pytz
import datetime
import requests
import sys
import googleapiclient.discovery
import atlassian

from google.auth import iam
from google.auth.transport import requests as gcp_requests
from google.oauth2 import service_account
from google.cloud import resource_manager, secretmanager, storage, bigquery
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound as GCP_NotFound
from google.api_core.exceptions import Forbidden as GCP_Forbidden
from googleapiclient.errors import HttpError as GCP_httperror

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
        self.resource_client = resource_manager.Client()
        self.stg_client = storage.Client(credentials=self.credentials)
        self.bq_client = bigquery.Client(credentials=self.credentials)
        self.publisher_client = pubsub_v1.PublisherClient(credentials=self.credentials)
        self.subscriber_client = pubsub_v1.SubscriberClient(credentials=self.credentials)
        self.su_client = googleapiclient.discovery.build(
            'serviceusage', 'v1', credentials=self.credentials, cache_discovery=False)
        self.sql_client = googleapiclient.discovery.build(
            'sqladmin', 'v1beta4', credentials=self.credentials, cache_discovery=False)

        self.project_services = {}

    def process(self):
        not_found_resources = []
        status = requests.head(self.ckan_host, verify=True).status_code
        if status != 200:
            logging.error('CKAN not reachable')
            return False
        # Get all projects on GCP that you have access to
        gcp_projects = []
        for project in self.resource_client.list_projects():
            gcp_projects.append(project.name)
        # Get all groups of CKAN, they are based on GCP project IDs
        group_list = self.host.action.group_list()
        # For every group
        for group_project_id in group_list:
            not_found_resource = NotFoundResource(group_project_id)
            # Check if project is in GCP projects
            if group_project_id not in gcp_projects:
                logging.info(f"Project ID {group_project_id} could not be found on GCP")
                resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
                not_found_resources.append(not_found_resource.make_not_found("Project not found", "google-cloud-project",
                                           group_project_id, "GCP Project", resource_url))
                continue
            # Get project's services
            gcp_services = self.get_project_services(group_project_id)
            group = self.get_project_group(group_project_id)
            # Get topics belonging to project ID
            not_found_resources, topics = self.get_topics(not_found_resource, gcp_services, not_found_resources, group_project_id)
            # Get subscriptions belonging to project ID
            not_found_resources, subscriptions = self.get_subscriptions(not_found_resource, gcp_services, not_found_resources,
                                                                        group_project_id)
            # Get buckets belonging to project ID
            not_found_resources, buckets = self.get_buckets(not_found_resource, gcp_services, not_found_resources, group_project_id)
            # Get SQL instances belonging to project ID
            not_found_resources, sql_instances = self.get_sql_instances(not_found_resource, gcp_services, not_found_resources,
                                                                        group_project_id)
            # Get SQL databases belonging to project ID
            not_found_resources, sql_databases = self.get_sql_databases(not_found_resource, gcp_services, sql_instances,
                                                                        not_found_resources, group_project_id)
            # Get bigquery datasets belonging to project ID
            not_found_resources, bigquery_datasets = self.get_bigquery_datasets(not_found_resource, gcp_services, not_found_resources,
                                                                                group_project_id)
            # For every package in the group
            for package in group.get('packages', []):
                full_package = self.host.action.package_show(id=package['id'])
                not_found_resources.extend(
                    self.Package(
                            package=full_package,
                            topics=topics,
                            subscriptions=subscriptions,
                            buckets=buckets,
                            sql_instances=sql_instances,
                            sql_databases=sql_databases,
                            bigquery_datasets=bigquery_datasets,
                            gcp_services=gcp_services,
                            group_project_id=group_project_id
                        ).process()
                    )
        self.subscriber_client.close()
        if len(not_found_resources) > 0:
            JiraProcessor(self.secret_client).create_issues(not_found_resources)
        return True

    def get_project_group(self, group_project_id):
        group = None
        try:
            group = self.host.action.group_show(id=group_project_id, include_datasets=True)
        except NotFound:  # Group does not exists
            logging.info(f"Group of {group_project_id} could not be found")
        except Exception:
            raise
        return group

    def get_project_services(self, group_project_id):
        try:
            response = self.su_client.services().list(parent=f"projects/{group_project_id}", filter="state:ENABLED").execute()
        except GCP_httperror as e:
            logging.info(f"Getting services from project with project ID {group_project_id} resulted in error {e}")
            response = {}
        services = [service.get('config').get('name') for service in response.get('services', [])]
        return services

    def get_subscriptions(self, not_found_resource, gcp_services, not_found_resources, group_project_id):
        project_path = f"projects/{group_project_id}"
        subscriptions = []
        # Check if project has pubsub service
        if "pubsub.googleapis.com" not in gcp_services:
            return not_found_resources, subscriptions
        try:
            for subscription in self.subscriber_client.list_subscriptions(
                request={"project": project_path}
            ):
                subscriptions.append(subscription.name.split('/')[-1])
        except GCP_NotFound:
            logging.info(f"Project ID {group_project_id} could not be found on GCP while getting subscriptions")
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(not_found_resource.make_not_found("Project not found", "google-cloud-project",
                                                                         group_project_id, "GCP Project", resource_url))
        return not_found_resources, subscriptions

    def get_topics(self, not_found_resource, gcp_services, not_found_resources, group_project_id):
        project_path = f"projects/{group_project_id}"
        topics = []
        # Check if project has pubsub service
        if "pubsub.googleapis.com" not in gcp_services:
            return not_found_resources, topics
        try:
            for topic in self.publisher_client.list_topics(request={"project": project_path}):
                topics.append(topic.name.split('/')[-1])
        except GCP_NotFound:
            logging.info(f"Project ID {group_project_id} could not be found on GCP while getting topics")
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(not_found_resource.make_not_found("Project not found", "google-cloud-project",
                                                                         group_project_id, "GCP Project", resource_url))
        return not_found_resources, topics

    def get_buckets(self, not_found_resource, gcp_services, not_found_resources, group_project_id):
        bucket_objects = self.stg_client.list_buckets(project=group_project_id)
        buckets = []
        # Check if project has storage service
        if "storage-api.googleapis.com" not in gcp_services:
            return not_found_resources, buckets
        try:
            for bucket_object in bucket_objects:
                buckets.append(bucket_object.name)
        except GCP_NotFound:
            logging.info(f"Project ID {group_project_id} could not be found on GCP while getting buckets")
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(not_found_resource.make_not_found("Project not found", "google-cloud-project",
                                                                         group_project_id, "GCP Project", resource_url))
        return not_found_resources, buckets

    def get_sql_instances(self, not_found_resource, gcp_services, not_found_resources, group_project_id):
        instances_response = {}
        instances = []
        # Check if project has sql service
        if "sqladmin.googleapis.com" not in gcp_services:
            return not_found_resources, instances
        try:
            instances_response = self.sql_client.instances().list(project=group_project_id).execute()
        except GCP_NotFound:
            logging.info(f"Project ID {group_project_id} could not be found on GCP while getting SQL instances")
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(not_found_resource.make_not_found("Project not found", "google-cloud-project",
                                                                         group_project_id, "GCP Project", resource_url))
        instances = [item['name'] for item in instances_response.get('items', [])]
        return not_found_resources, instances

    def get_sql_databases(self, not_found_resource, gcp_services, instances, not_found_resources, group_project_id):
        resources = []
        databases = []
        # Check if project has sql service
        if "sqladmin.googleapis.com" not in gcp_services:
            return not_found_resources, resources
        for instance in instances:
            try:
                databases = self.sql_client.databases().list(project=group_project_id, instance=instance).execute()
            except GCP_NotFound:
                logging.info(f"Project ID {group_project_id} could not be found on GCP while getting SQL databases")
                resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
                not_found_resources.append(not_found_resource.make_not_found("Project not found", "google-cloud-project",
                                                                             group_project_id, "GCP Project", resource_url))
            for database in databases.get('items', []):
                resources.append(database['name'])
        return not_found_resources, resources

    def get_bigquery_datasets(self, not_found_resource, gcp_services, not_found_resources, group_project_id):
        datasets_list = []
        datasets = []
        # Check if project has bigquery service
        if "bigquery.googleapis.com" not in gcp_services:
            return not_found_resources, datasets
        try:
            datasets_list = list(self.bq_client.list_datasets(project=group_project_id))
        except GCP_NotFound:
            logging.info(f"Project ID {group_project_id} could not be found on GCP while getting bigquery datasets")
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(not_found_resource.make_not_found("Project not found", "google-cloud-project", group_project_id,
                                                                         "GCP Project", resource_url))
        except GCP_Forbidden:
            logging.info(f"Listing services of Project ID {group_project_id} resulted in bigquery API but it is not used")
            # TODO: for now returning list with True because setting the project variable in the BigQuery datasets call
            # does not work, it still gets the credentials project
            # When this is fixed, the right datasets can be returned
            return not_found_resources, [True]
        datasets = [dataset.dataset_id for dataset in datasets_list]
        return not_found_resources, datasets

    class Package(object):
        def __init__(self, package, group_project_id, subscriptions, topics, buckets,
                     sql_instances, sql_databases, bigquery_datasets, gcp_services):
            self.package = package
            self.topics = topics
            self.subscriptions = subscriptions
            self.buckets = buckets
            self.sql_instances = sql_instances
            self.sql_databases = sql_databases
            self.bigquery_datasets = bigquery_datasets
            self.gcp_services = gcp_services
            self.group_project_id = group_project_id

            self.package_name = package.get('name', '').replace('_', '-')
            self.not_found_resource = NotFoundResource(group_project_id)
            self.not_found_resources = []

        def process(self):
            if self.package.get('resources', None):
                for resource in self.package.get('resources', []):
                    if 'format' in resource and 'name' in resource:
                        resource_name = resource['name']
                        resource_format = resource['format']
                        success = False
                        if resource_format == 'subscription':
                            success = self.check_subscriptions(resource_name)
                        elif resource_format == 'topic':
                            success = self.check_topics(resource_name)
                        elif resource_format == 'blob-storage':
                            success = self.check_list(resource_name, self.buckets)
                        elif resource_format == 'cloudsql-instance':
                            success = self.check_list(resource_name, self.sql_instances)
                        elif resource_format == 'cloudsql-db':
                            success = self.check_list(resource_name, self.sql_databases)
                        elif resource_format == 'bigquery-dataset':
                            success = self.check_list(resource_name, self.bigquery_datasets)
                            # TODO: remove below when the BigQuery API can set a project correctly,
                            # see TODO in get_bigquery_datasets function
                            if len(self.bigquery_datasets) == 1:
                                if self.bigquery_datasets[0] is True:
                                    success = True
                            #############################################
                        elif resource_format == 'API':
                            success = self.check_api(resource)
                        elif resource_format in ['datastore', 'datastore-index']:
                            success = self.check_service('datastore.googleapis.com')
                        elif resource_format == 'firestore':
                            success = self.check_service('firestore.googleapis.com')
                        else:
                            logging.debug(f"Skipping resource '{resource_name}' with format '{resource['format']}'")
                            continue
                        if success is False:
                            resource_url = resource.get('url', '')
                            if resource_url:
                                resource_url = f"{resource_url}?project={self.group_project_id}"
                            self.not_found_resources.append(
                                self.not_found_resource.make_not_found(
                                    "Resource not found",
                                    self.package_name,
                                    resource_name,
                                    resource['format'],
                                    resource_url
                                )
                            )
                    else:
                        logging.info(f"Resource does not have the correct fields: {resource}")
            else:
                logging.info(f"Dataset '{self.package_name}' does not have any resources")
            return self.not_found_resources

        def check_subscriptions(self, subscription_name):
            if subscription_name.split('/')[-1] not in self.subscriptions:
                return False
            return True

        def check_topics(self, topic_name):
            if topic_name.split('/')[-1] not in self.topics:
                return False
            return True

        def check_list(self, resource_name, resources):
            for resource in resources:
                if resource == resource_name:
                    return True
            return False

        def check_api(self, resource):
            if 'url' in resource:
                response = requests.get(resource['url'])
                if not response.ok:
                    return False
            return True

        def check_service(self, service_name):
            if service_name not in self.gcp_services:
                return False
            return True


class NotFoundResource(object):
    def __init__(self, group_project_id):
        self.group_project_id = group_project_id

    def make_not_found(self, message, package_name, resource_name, resource_type, resource_url):
        timezone = pytz.timezone("Europe/Amsterdam")
        timestamp = datetime.datetime.now(tz=timezone)
        not_found_dict = {
            "message": message,
            "project_id": self.group_project_id,
            "package_name": package_name,
            "resource_name": resource_name,
            "type": resource_type,
            "access_url": resource_url,
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }
        return not_found_dict


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
            logging.info(
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


def check_catalog_existence(request):
    logging.info("Initialized function")

    if 'PROJECT_ID' in os.environ and \
       'CKAN_API_KEY_SECRET_ID' in os.environ and \
       'CKAN_SITE_URL' in os.environ and hasattr(config, 'DELEGATED_SA'):
        proccess_bool = CKANProcessor().process()
        if proccess_bool is False:
            logging.info("Catalog existence check has not run")
        else:
            logging.info("Catalog existence check has run")
    else:
        logging.error('Function has insufficient configuration')


if __name__ == '__main__':
    check_catalog_existence(None)
