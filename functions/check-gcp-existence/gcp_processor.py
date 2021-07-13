import logging

import config
from ckan_service import CKANService
from gcp_helper import GCPHelper
from gcp_service import GCPService
from gobits import Gobits
from not_found_resource import NotFoundResource


class GCPProcessor(object):
    def __init__(self):
        self.gcp_helper = GCPHelper()
        self.gcp_service = GCPService()
        self.ckan_service = CKANService()

    def process_not_found_projects(self, not_found_projects):
        not_found_resources = []
        for project_id in not_found_projects:
            logging.info(
                f"Project ID {project_id} could not be found on CKAN while it still exists in GCP"
            )
            resource_url = f"https://console.cloud.google.com/home/dashboard?project={project_id}"
            not_found_resources.append(
                NotFoundResource(project_id).make_not_found(
                    "Project not found",
                    "google-cloud-project",
                    project_id,
                    "GCP Project",
                    resource_url,
                )
            )
        return not_found_resources

    def is_default_resource(self, name):
        keywords = ['.appspot.com', 'cloud-builds', 'gcf-sources', '_cloudbuild']
        return_value = False
        for k in keywords:
            if k in name:
                return_value = True
                break
        return return_value

    def process(self, request):
        if not self.ckan_service.is_ckan_reachable():
            return False

        # get GCP projects
        project_list = self.gcp_service.get_projects()
        # get all groups of CKAN, they are based on GCP project IDs
        group_list = self.ckan_service.get_group_list()
        # get mismatching projects
        mismatching_projects = list(set(project_list) - set(group_list))
        not_found_resources = self.process_not_found_projects(mismatching_projects)
        # get matching projects
        matching_projects = list(set(project_list) - set(mismatching_projects))
        for project_id in matching_projects:
            not_found_resource = NotFoundResource(project_id)
            # Get project's services
            gcp_services = self.gcp_service.get_project_services(project_id)
            # Get matching ckan project
            group = self.ckan_service.get_project_group(project_id)
            # get ckan resources
            ckan_resources = []
            ckan_resources_search = ''
            for package in group.get("packages", []):
                ckan_resources = self.ckan_service.get_full_package(package["id"]).get("resources", [])
                for resource in ckan_resources:
                    if "format" in resource and "name" in resource:
                        ckan_resources_search += ':' + resource["name"] + ':' + resource["format"]
            # Get topics belonging to project ID
            not_found_resources, topics = self.gcp_service.get_topics(
                not_found_resource, gcp_services, not_found_resources, project_id
            )

            # Get subscriptions belonging to project ID
            not_found_resources, subscriptions = self.gcp_service.get_subscriptions(
                not_found_resource, gcp_services, not_found_resources, project_id
            )
            # Get buckets belonging to project ID
            not_found_resources, buckets = self.gcp_service.get_buckets(
                not_found_resource, gcp_services, not_found_resources, project_id
            )
            # Get SQL instances belonging to project ID
            not_found_resources, sql_instances = self.gcp_service.get_sql_instances(
                not_found_resource, gcp_services, not_found_resources, project_id
            )
            # Get SQL databases belonging to project ID
            not_found_resources, sql_databases = self.gcp_service.get_sql_databases(
                not_found_resource,
                gcp_services,
                sql_instances,
                not_found_resources,
                project_id,
            )
            # Get bigquery datasets belonging to project ID
            not_found_resources, bigquery_datasets = self.gcp_service.get_bigquery_datasets(
                not_found_resource, gcp_services, not_found_resources, project_id
            )

            resources = {
                'topic': topics,
                'subscription': subscriptions,
                'blob-storage': buckets,
                'cloudsql-instance': sql_instances,
                'cloudsql-db': sql_databases,
                'bigquery-dataset': bigquery_datasets
            }
            for key, value in resources.items():
                for resource_name in value:
                    search = ':' + resource_name + ':' + key + ':'
                    if search not in ckan_resources_search and not self.is_default_resource(resource_name):
                        logging.info(
                            f"Resource {resource_name} could not be found on CKAN while it still exists in GCP"
                        )
                        not_found_resources.append(
                            not_found_resource.make_not_found(
                                "Resource not found",
                                '',  # package name is a ckan-specific attribute
                                resource_name,
                                key,
                                self.gcp_service.generate_resource_url(key, resource_name, project_id),
                            )
                        )
        self.gcp_service.get_subscriber_client().close()

        # Create gobits object
        metadata = Gobits.from_request(request=request)
        # Send issues to a topic
        return self.gcp_helper.publish_to_topic(
            config.TOPIC_PROJECT_ID, config.TOPIC_NAME,
            not_found_resources, [metadata.to_json()]
        )
