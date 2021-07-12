import logging

import config
from ckan_service import CKANService
from gcp_helper import GCPHelper
from gcp_service import GCPService
from gobits import Gobits
from not_found_resource import NotFoundResource
from package import Package


class CKANProcessor(object):
    def __init__(self):
        self.gcp_helper = GCPHelper()
        self.gcp_service = GCPService()
        self.ckan_service = CKANService()

    def process(self, request):
        not_found_resources = []
        if not self.ckan_service.is_ckan_reachable():
            return False
        # Get all groups of CKAN, they are based on GCP project IDs
        group_list = self.ckan_service.get_group_list()
        # For every group
        for group_project_id in group_list:
            not_found_resource = NotFoundResource(group_project_id)
            # Get project's services
            gcp_services = self.gcp_service.get_project_services(group_project_id)
            # If no gcp_services where found, the project does not exist
            if not gcp_services:
                logging.info(
                    f"Project ID {group_project_id} could not be found on GCP while getting services"
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
            group = self.ckan_service.get_project_group(group_project_id)
            # Get topics belonging to project ID
            not_found_resources, topics = self.gcp_service.get_topics(
                not_found_resource, gcp_services, not_found_resources, group_project_id
            )
            # Get subscriptions belonging to project ID
            not_found_resources, subscriptions = self.gcp_service.get_subscriptions(
                not_found_resource, gcp_services, not_found_resources, group_project_id
            )
            # Get buckets belonging to project ID
            not_found_resources, buckets = self.gcp_service.get_buckets(
                not_found_resource, gcp_services, not_found_resources, group_project_id
            )
            # Get SQL instances belonging to project ID
            not_found_resources, sql_instances = self.gcp_service.get_sql_instances(
                not_found_resource, gcp_services, not_found_resources, group_project_id
            )
            # Get SQL databases belonging to project ID
            not_found_resources, sql_databases = self.gcp_service.get_sql_databases(
                not_found_resource,
                gcp_services,
                sql_instances,
                not_found_resources,
                group_project_id,
            )
            # Get bigquery datasets belonging to project ID
            not_found_resources, bigquery_datasets = self.gcp_service.get_bigquery_datasets(
                not_found_resource, gcp_services, not_found_resources, group_project_id
            )
            # For every package in the group
            for package in group.get("packages", []):
                full_package = self.ckan_service.get_full_package(package["id"])
                not_found_resources.extend(
                    Package(
                        package=full_package,
                        topics=topics,
                        subscriptions=subscriptions,
                        buckets=buckets,
                        sql_instances=sql_instances,
                        sql_databases=sql_databases,
                        bigquery_datasets=bigquery_datasets,
                        gcp_services=gcp_services,
                        group_project_id=group_project_id,
                    ).process()
                )
        self.gcp_service.get_subscriber_client().close()

        # Create gobits object
        metadata = Gobits.from_request(request=request)

        # Send issues to a topic
        return self.gcp_helper.publish_to_topic(
            config.TOPIC_PROJECT_ID, config.TOPIC_NAME,
            not_found_resources, [metadata.to_json()]
        )
