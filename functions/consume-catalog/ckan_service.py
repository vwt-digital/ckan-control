import os
from ckanapi import NotFound, RemoteCKAN, SearchError, ValidationError
from google.cloud import secretmanager
import logging
import requests


class CKANService:
    def __init__(self):
        self.project_id = os.environ.get("PROJECT_ID", "Required parameter is missing")
        self.api_key_secret_id = os.environ.get(
            "API_KEY_SECRET_ID", "Required parameter is missing"
        )
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{self.project_id}/secrets/{self.api_key_secret_id}/versions/latest"
        key_response = client.access_secret_version(request={"name": secret_name})
        self.api_key = key_response.payload.data.decode("UTF-8")
        self.ckan_host = os.environ.get(
            "CKAN_SITE_URL", "Required parameter is missing"
        )
        self.session = requests.Session()
        self.session.verify = True
        self.host = RemoteCKAN(
            self.ckan_host, apikey=self.api_key, session=self.session
        )

    def delete_resources(self, selector_data, group, future_packages_list):
        # Deleting resources existing in CKAN but not in data-catalog based on Project ID
        if "projectId" in selector_data:
            current_packages_list = []
            for package in self.host.action.group_show(
                    id=group["id"], include_datasets=True
            ).get("packages", []):
                if package["project_id"] == group["name"]:
                    current_packages_list.append(package["name"])

            packages_to_delete = list(
                set(current_packages_list).difference(future_packages_list)
            )
            logging.info(
                f"Deleting {len(packages_to_delete)} non-existing group-packages"
            )
            for package_name in packages_to_delete:
                self.purge_dataset(package_name)

    def get_project_group(self, catalog):
        group = None

        if "projectId" in catalog:
            project_id = catalog["projectId"]
            try:
                group = self.host.action.group_show(id=project_id)
            except NotFound:  # Group does not exists
                logging.info(f"Creating new group '{project_id}'")
                group = self.host.action.group_create(name=project_id)
            except Exception:
                raise

        return group

    def create_tag_dict(self, catalog):
        tag_dict = []
        for name in ["domain", "solution"]:
            vocabulary = None
            try:  # Check if correct vocabulary tags exist
                vocabulary = self.host.action.vocabulary_show(id=name)
            except NotFound:
                vocabulary = self.host.action.vocabulary_create(name=name)
            except Exception:
                raise

            # Create package's tags list
            if vocabulary and name in catalog:
                if catalog[name] not in self.host.action.tag_list(
                        vocabulary_id=vocabulary["id"]
                ):
                    tag = self.host.action.tag_create(
                        name=catalog[name], vocabulary_id=vocabulary["id"]
                    )
                    logging.info(f"Created '{name}' tag for '{catalog[name]}'")
                else:
                    tag = self.host.action.tag_show(
                        id=catalog[name], vocabulary_id=vocabulary["id"]
                    )

                tag_dict.append(tag)

        return tag_dict

    def patch_dataset(self, data_dict):
        logging.info(f"Patching dataset '{data_dict['name']}'")
        current_resources_list = {}
        try:
            cur_package = self.host.action.package_show(
                id=data_dict["name"]
            )  # Retrieve package

            for resource in cur_package[
                "resources"
            ]:  # Create list with package resources
                current_resources_list[resource["name"]] = resource

            data_dict["id"] = cur_package.get("id")  # Set package id for patching
            self.host.action.package_patch(
                id=data_dict["id"], data_dict=data_dict
            )  # Patch package
        except NotFound:
            logging.info(f"Creating dataset '{data_dict['name']}'")
            self.host.action.package_create(
                name=data_dict["name"],
                data_dict=data_dict,
                owner_org=data_dict["owner_org"],
            )  # Create package if not-existing
        except Exception:
            raise

        return current_resources_list

    def purge_dataset(self, package_name):
        logging.info(f"Purging dataset '{package_name}' and it's resources")

        try:
            package = self.host.action.package_show(id=package_name)  # Retrieve package

            for resource in package.get("resources", []):  # Delete package resources
                self.host.action.resource_delete(id=resource["id"])

            self.host.action.dataset_purge(id=package["id"])  # Purge package
        except NotFound:
            pass
        except Exception:
            raise

    def process_resources(
            self, data_dict, to_create, to_update, to_delete, current_list, future_list
    ):
        logging.info(
            "{} new, {} existing and {} old resources for dataset '{}'".format(
                len(to_create), len(to_update), len(to_delete), data_dict["name"]
            )
        )
        self.patch_resources(to_update, future_list, current_list, to_create)
        self.create_resources(to_create, future_list)
        self.delete_resources_ckan(to_delete, current_list)

    def patch_resources(self, to_update, future_list, current_list, to_create):
        # Patch resources
        for name in to_update:
            resource = future_list.get(name)
            resource["id"] = current_list.get(name).get("id")

            try:
                logging.info(f"Patching resource '{resource['name']}'")
                self.host.action.resource_patch(**resource)
            except NotFound:  # Resource does not exist
                logging.info(
                    f"Resource '{resource['name']}' does not exist, adding to 'to_create'"
                )
                to_create.append(resource["name"])
            except SearchError:
                logging.error(
                    f"SearchError occured while patching resource '{resource['name']}'"
                )

    def create_resources(self, to_create, future_list):
        # Create resources
        for name in to_create:
            resource = future_list.get(name)
            try:
                logging.info(f"Creating resource '{resource['name']}'")
                self.host.action.resource_create(**resource)
            except ValidationError:  # Resource already exists
                logging.info(
                    f"Resource '{resource['name']}' already exists, patching resource"
                )
                resource["id"] = self.host.action.resource_show(id=name).get("id")
                self.host.action.resource_patch(**resource)
            except SearchError:
                logging.error(
                    f"SearchError occured while creating resource '{resource['name']}'"
                )

    def delete_resources_ckan(self, to_delete, current_list):
        # Delete resources
        for name in to_delete:
            resource = current_list.get(name)

            try:
                logging.info(f"Deleting resource '{resource['name']}'")
                self.host.action.resource_delete(id=resource["id"])
            except Exception as e:  # An exception occurred
                logging.error(
                    f"Exception occurred while deleting resource '{resource['name']}': {e}"
                )
                pass
