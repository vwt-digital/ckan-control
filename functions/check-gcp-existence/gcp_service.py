import logging
import re

import config
import googleapiclient.discovery
from gcp_helper import GCPHelper
from google.api_core.exceptions import BadRequest as GCP_BadRequest
from google.api_core.exceptions import Forbidden as GCP_Forbidden
from google.api_core.exceptions import NotFound as GCP_NotFound
from google.cloud import bigquery, pubsub_v1, storage
from googleapiclient.errors import HttpError as GCP_httperror


class GCPService:

    def __init__(self):
        self.gcp_helper = GCPHelper()
        self.credentials = self.gcp_helper.request_auth_token()
        self.stg_client = storage.Client(credentials=self.credentials)
        self.bq_client = bigquery.Client(credentials=self.credentials)
        self.publisher_client = pubsub_v1.PublisherClient(credentials=self.credentials)
        self.subscriber_client = pubsub_v1.SubscriberClient(
            credentials=self.credentials
        )
        self.su_client = googleapiclient.discovery.build(
            "serviceusage", "v1", credentials=self.credentials, cache_discovery=False
        )
        self.sql_client = googleapiclient.discovery.build(
            "sqladmin", "v1beta4", credentials=self.credentials, cache_discovery=False
        )
        #
        self.crm_client = googleapiclient.discovery.build(
            "cloudresourcemanager", "v1", credentials=self.credentials, cache_discovery=False
        )

    def get_project_services(self, group_project_id):
        try:
            response = (
                self.su_client.services()
                    .list(parent=f"projects/{group_project_id}", filter="state:ENABLED")
                    .execute()
            )
        except GCP_httperror as e:
            logging.info(
                f"Getting services from project with project ID {group_project_id} resulted in error {e}"
            )
            response = {}
        services = [
            service.get("config").get("name")
            for service in response.get("services", [])
        ]
        return services

    def get_subscriptions(
            self, not_found_resource, gcp_services, not_found_resources, group_project_id
    ):
        project_path = f"projects/{group_project_id}"
        subscriptions = []
        # Check if project has pubsub service
        if "pubsub.googleapis.com" not in gcp_services:
            return not_found_resources, subscriptions
        try:
            for subscription in self.subscriber_client.list_subscriptions(
                    request={"project": project_path}
            ):
                subscriptions.append(subscription.name.split("/")[-1])
        except GCP_NotFound:
            logging.info(
                f"Project ID {group_project_id} could not be found on GCP while getting subscriptions"
            )
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(
                not_found_resource.make_not_found(
                    "Project not found",
                    "google-cloud-project",
                    group_project_id,
                    "GCP Project",
                    resource_url,
                )
            )
        return not_found_resources, subscriptions

    def get_topics(
            self, not_found_resource, gcp_services, not_found_resources, group_project_id
    ):
        project_path = f"projects/{group_project_id}"
        topics = []
        # Check if project has pubsub service
        if "pubsub.googleapis.com" not in gcp_services:
            return not_found_resources, topics
        try:
            for topic in self.publisher_client.list_topics(
                    request={"project": project_path}
            ):
                topics.append(topic.name.split("/")[-1])
        except GCP_NotFound:
            logging.info(
                f"Project ID {group_project_id} could not be found on GCP while getting topics"
            )
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(
                not_found_resource.make_not_found(
                    "Project not found",
                    "google-cloud-project",
                    group_project_id,
                    "GCP Project",
                    resource_url,
                )
            )
        return not_found_resources, topics

    def get_buckets(
            self, not_found_resource, gcp_services, not_found_resources, group_project_id
    ):
        bucket_objects = self.stg_client.list_buckets(project=group_project_id)
        buckets = []
        # Check if project has storage service
        if "storage-api.googleapis.com" not in gcp_services:
            return not_found_resources, buckets
        try:
            for bucket_object in bucket_objects:
                buckets.append(bucket_object.name)
        except GCP_NotFound:
            logging.info(
                f"Project ID {group_project_id} could not be found on GCP while getting buckets"
            )
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(
                not_found_resource.make_not_found(
                    "Project not found",
                    "google-cloud-project",
                    group_project_id,
                    "GCP Project",
                    resource_url,
                )
            )
        return not_found_resources, buckets

    def get_sql_instances(
            self, not_found_resource, gcp_services, not_found_resources, group_project_id
    ):
        instances_response = {}
        instances = []
        # Check if project has sql service
        if "sqladmin.googleapis.com" not in gcp_services:
            return not_found_resources, instances
        try:
            instances_response = (
                self.sql_client.instances().list(project=group_project_id).execute()
            )
        except GCP_NotFound:
            logging.info(
                f"Project ID {group_project_id} could not be found on GCP while getting SQL instances"
            )
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(
                not_found_resource.make_not_found(
                    "Project not found",
                    "google-cloud-project",
                    group_project_id,
                    "GCP Project",
                    resource_url,
                )
            )
        instances = [item["name"] for item in instances_response.get("items", [])]
        return not_found_resources, instances

    def get_sql_databases(
            self,
            not_found_resource,
            gcp_services,
            instances,
            not_found_resources,
            group_project_id,
    ):
        resources = []
        databases = []
        # Check if project has sql service
        if "sqladmin.googleapis.com" not in gcp_services:
            return not_found_resources, resources
        for instance in instances:
            try:
                databases = (
                    self.sql_client.databases()
                        .list(project=group_project_id, instance=instance)
                        .execute()
                )
            except GCP_NotFound:
                logging.info(
                    f"Project ID {group_project_id} could not be found on GCP while getting SQL databases"
                )
                resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
                not_found_resources.append(
                    not_found_resource.make_not_found(
                        "Project not found",
                        "google-cloud-project",
                        group_project_id,
                        "GCP Project",
                        resource_url,
                    )
                )
            for database in databases.get("items", []):
                resources.append(database["name"])
        return not_found_resources, resources

    def get_bigquery_datasets(
            self, not_found_resource, gcp_services, not_found_resources, group_project_id
    ):
        datasets_list = []
        datasets = []
        # Check if project has bigquery service
        if "bigquery.googleapis.com" not in gcp_services:
            return not_found_resources, datasets
        try:
            datasets_list = list(self.bq_client.list_datasets(project=group_project_id))
        except GCP_NotFound:
            logging.info(
                f"Project ID {group_project_id} could not be found on GCP while getting bigquery datasets"
            )
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={group_project_id}"
            not_found_resources.append(
                not_found_resource.make_not_found(
                    "Project not found",
                    "google-cloud-project",
                    group_project_id,
                    "GCP Project",
                    resource_url,
                )
            )
        except GCP_Forbidden as e:
            logging.info(
                f"Listing services of Project ID {group_project_id} resulted in bigquery API but an error occured: {e}"
            )
        except GCP_BadRequest as e:
            logging.info(
                f"Listing services of Project ID {group_project_id} resulted in bigquery API but an error occured: {e}"
            )
        datasets = [dataset.dataset_id for dataset in datasets_list]
        return not_found_resources, datasets

    def get_subscriber_client(self):
        return self.subscriber_client

    #
    def get_projects(self):
        try:
            response = (
                self.crm_client.projects()
                    .list()
                    .execute()
            )
        except GCP_httperror as e:
            logging.info(
                f"Getting GCP projects resulted in error {e}"
            )
            response = {}
        projects = [
            project.get("name")
            for project in response.get("projects", [])
        ]
        # filter projects according to environment
        topic_name = config.TOPIC_NAME
        env = re.search('-[p|d]-', topic_name)[0]
        prefix = topic_name.partition(env)[0]
        env = env.replace('-', '')
        r = re.compile("{}-{}-*".format(prefix, env))
        projects = list(filter(r.match, projects))
        return projects

    def generate_resource_url(self, resource_type, name, project_id):
        base_url = 'https://console.cloud.google.com'
        return {
            'topic': f'{base_url}/cloudpubsub/topic/detail/{name}?project={project_id}',
            'subscription': f'{base_url}/cloudpubsub/subscription/detail/{name}?project={project_id}',
            'blob-storage': f'{base_url}/storage/browser/{name}?project={project_id}',
            'cloudsql-instance': f'{base_url}/sql/instances/{name}?project={project_id}',
            'cloudsql-db': f'{base_url}/sql/databases/{name}?project={project_id}',
            'bigquery-dataset': f'{base_url}/bigquery/{name}?project={project_id}',
        }.get(resource_type, '')
