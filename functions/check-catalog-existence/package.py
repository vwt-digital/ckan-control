import logging

import requests
from not_found_resource import NotFoundResource


class Package(object):
    def __init__(
            self,
            package,
            group_project_id,
            subscriptions,
            topics,
            buckets,
            sql_instances,
            sql_databases,
            bigquery_datasets,
            gcp_services,
    ):
        self.package = package
        self.topics = topics
        self.subscriptions = subscriptions
        self.buckets = buckets
        self.sql_instances = sql_instances
        self.sql_databases = sql_databases
        self.bigquery_datasets = bigquery_datasets
        self.gcp_services = gcp_services
        self.group_project_id = group_project_id

        self.package_name = package.get("name", "").replace("_", "-")
        self.not_found_resource = NotFoundResource(group_project_id)
        self.not_found_resources = []

    def process(self):
        if self.package.get("resources", None):
            for resource in self.package.get("resources", []):
                if "format" in resource and "name" in resource:
                    resource_name = resource["name"]
                    resource_format = resource["format"]
                    success = self.check_resource_format(
                        resource_format, resource_name, resource
                    )
                    if isinstance(success, str):
                        continue
                    if success is False:
                        resource_url = resource.get("url", "")
                        if resource_url:
                            resource_url = (
                                f"{resource_url}?project={self.group_project_id}"
                            )
                        self.not_found_resources.append(
                            self.not_found_resource.make_not_found(
                                "Resource not found",
                                self.package_name,
                                resource_name,
                                resource["format"],
                                resource_url,
                            )
                        )
                else:
                    logging.info(
                        f"Resource does not have the correct fields: {resource}"
                    )
        else:
            logging.info(
                f"Dataset '{self.package_name}' does not have any resources"
            )
        return self.not_found_resources

    def check_resource_format(self, resource_format, resource_name, resource):
        success = False
        if resource_format == "subscription":
            success = self.check_subscriptions(resource_name)
        elif resource_format == "topic":
            success = self.check_topics(resource_name)
        elif resource_format == "blob-storage":
            success = self.check_list(resource_name, self.buckets)
        elif resource_format == "cloudsql-instance":
            success = self.check_list(resource_name, self.sql_instances)
        elif resource_format == "cloudsql-db":
            success = self.check_list(resource_name, self.sql_databases)
        elif resource_format == "bigquery-dataset":
            success = self.check_list(resource_name, self.bigquery_datasets)
        elif resource_format == "API":
            success = self.check_api(resource)
        elif resource_format in ["datastore", "datastore-index"]:
            success = self.check_service("datastore.googleapis.com")
        elif resource_format == "firestore":
            success = self.check_service("firestore.googleapis.com")
        else:
            logging.debug(
                f"Skipping resource '{resource_name}' with format '{resource['format']}'"
            )
            success = "continue"
        return success

    def check_subscriptions(self, subscription_name):
        return subscription_name.split("/")[-1] in self.subscriptions

    def check_topics(self, topic_name):
        return topic_name.split("/")[-1] in self.topics

    def check_api(self, resource):
        if "url" in resource:
            response = requests.get(resource["url"])
            if not response.ok:
                return False
        return True

    def check_service(self, service_name):
        return service_name in self.gcp_services

    def check_list(self, resource_name, resources):
        for resource in resources:
            if resource == resource_name:
                return True
        return False
