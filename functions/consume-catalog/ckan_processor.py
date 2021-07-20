import logging
import os

import urllib3
from ckan_service import CKANService
from gcp_service import GCPService

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class CKANProcessor(object):
    def __init__(self):
        self.ckan_service = CKANService()
        self.gcp_service = GCPService()

    def process(self, payload):
        selector_data = payload[
            os.environ.get("DATA_SELECTOR", "Required parameter is missing")
        ]
        future_packages_list = []

        group = self.ckan_service.get_project_group(selector_data)
        tag_dict = self.ckan_service.create_tag_dict(selector_data)

        if len(selector_data.get("dataset", [])) > 0:
            for data in selector_data["dataset"]:
                # Put the details of the dataset we're going to create into a dict
                dict_list = [
                    {"key": "Access Level", "value": data.get("accessLevel")},
                    {"key": "Issued", "value": data.get("issued")},
                    {"key": "Spatial", "value": data.get("spatial")},
                    {"key": "Modified", "value": data.get("modified")},
                    {"key": "Publisher", "value": data.get("publisher").get("name")},
                    {
                        "key": "Keywords",
                        "value": ", ".join(data.get("keyword"))
                        if "keyword" in data
                        else "",
                    },
                    {"key": "Temporal", "value": data.get("temporal")},
                    {
                        "key": "Accrual Periodicity",
                        "value": data.get("accrualPeriodicity"),
                    },
                ]

                data_dict = {
                    "name": data["identifier"],
                    "title": data["title"],
                    "notes": data["rights"],
                    "owner_org": "dat",
                    "maintainer": data.get("contactPoint").get("fn"),
                    "project_id": selector_data.get("projectId"),
                    "state": "active",
                    "tags": tag_dict,
                    "groups": [group],
                    "extras": dict_list,
                }
                # name is used for url and cannot have uppercase or spaces so we have to replace those
                data_dict["name"] = (
                    data_dict["name"].replace("/", "_").replace(".", "-").lower()
                )
                future_packages_list.append(data_dict["name"])

                # Create list with future resources
                future_resources_list = {}
                for resource in data["distribution"]:
                    resource_dict = {
                        "package_id": data_dict["name"],
                        "url": resource["accessURL"],
                        "description": resource.get("description", ""),
                        "name": resource["title"],
                        "format": resource["format"],
                        "mediaType": resource.get("mediaType", ""),
                    }
                    # Check if resource has a "describedBy" because then it has a schema
                    if "describedBy" in resource:
                        # Add the tag to the resource
                        resource_dict["schema_tag"] = resource["describedBy"]
                        # Check if the schema is already in the schemas storage
                        schema = self.gcp_service.check_schema_stg(resource["describedBy"])
                        # If it is
                        if schema:
                            # Add it to the resource
                            resource_dict["schema"] = schema
                    if resource["title"] not in future_resources_list:
                        future_resources_list[resource["title"]] = resource_dict
                    else:
                        logging.error(
                            f"'{resource['title']}' already exists, rename this resource"
                        )
                        continue

                # Create lists to create, update and delete
                current_resources_list = self.ckan_service.patch_dataset(data_dict)

                resources_to_create = list(
                    set(future_resources_list).difference(current_resources_list)
                )
                resources_to_update = list(
                    set(future_resources_list).intersection(current_resources_list)
                )
                resources_to_delete = list(
                    set(current_resources_list).difference(future_resources_list)
                )

                self.ckan_service.process_resources(
                    data_dict=data_dict,
                    to_create=resources_to_create,
                    to_update=resources_to_update,
                    to_delete=resources_to_delete,
                    current_list=current_resources_list,
                    future_list=future_resources_list,
                )

        else:
            logging.info("JSON request does not contain a dataset")
        self.ckan_service.delete_resources(selector_data, group, future_packages_list)
