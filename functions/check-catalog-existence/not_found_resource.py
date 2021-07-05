import datetime

import pytz


class NotFoundResource(object):
    def __init__(self, group_project_id):
        self.group_project_id = group_project_id

    def make_not_found(
            self, message, package_name, resource_name, resource_type, resource_url
    ):
        timezone = pytz.timezone("Europe/Amsterdam")
        timestamp = datetime.datetime.now(tz=timezone)
        not_found_dict = {
            "message": message,
            "project_id": self.group_project_id,
            "package_name": package_name,
            "resource_name": resource_name,
            "type": resource_type,
            "access_url": resource_url,
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        return not_found_dict
