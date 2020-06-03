import os
import logging
import requests
import json
import re

from ckanapi import RemoteCKAN, NotFound

logging.basicConfig(level=logging.INFO)


class CKANProcessor(object):
    def __init__(self):
        self.ckan_api_key = os.environ.get('CKAN_API_KEY', 'Required parameter is missing')
        self.ckan_host = os.environ.get('CKAN_SITE_URL', 'Required parameter is missing')
        self.host = RemoteCKAN(self.ckan_host, apikey=self.ckan_api_key)
        self.github_header = {"Authorization": "Bearer " + os.environ.get('GITHUB_API_KEY', 'Required parameter is missing')}

        # GitHub url format: 'https://github.com/<org>/<repo>/blob/<branch>/<path>'
        self.re_github_url = r"(?:https://github.com/)(vwt-digital|vwt-digital-config+)(?:/)([\w-]+)(?:/blob/)(master|develop+)(?:/)(.+)"

    def process(self):
        if self.host.action.site_read():
            try:
                package_list = self.host.action.package_list()
            except Exception:
                raise

            logging.info(f"Checking data-catalog existence for {len(package_list)} packages")
            packages_without_url = 0

            for key in package_list:
                try:
                    package = self.host.action.package_show(id=key)
                except Exception as e:
                    logging.info(f"Failed to retrieve '{key}' package: {str(e)}")
                    pass
                    continue

                if 'github_url' in package:
                    name = package['name'].replace("_", "/")
                    github_url = package['github_url']

                    if re.search(self.re_github_url, github_url):
                        match = re.match(self.re_github_url, github_url)

                        owner = match.group(1)
                        repo = match.group(2)
                        branch = match.group(3)
                        path = match.group(4)

                        url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
                        params = {"ref": branch}

                        try:
                            response = requests.get(url, params=params, headers=self.github_header)
                        except Exception as e:
                            logging.error(f"Exception on package '{name}': {str(e)}")
                        else:
                            if response.status_code in [200, 404]:
                                data = json.loads(response.content)

                                if 'html_url' in data and data['html_url'] == github_url:
                                    logging.info(f"Found data-catalog for package '{name}'")
                                    continue

                                self.delete_package_and_resources(package)  # Deleting package and it's resources
                                continue

                            logging.info(f"Status code {response.status_code} for package '{name}'")
                    else:
                        logging.error(f"Package '{name}' has an incorrect GitHub Url, the format is " +
                                      f"[https://github.com/<org>/<repo>/blob/<branch>/<path>]: {github_url}")
                else:
                    packages_without_url += 1

            logging.info(f"A total of {packages_without_url} of {len(package_list)} packages have no GitHub Url")
        else:
            logging.error('CKAN not reachable')

    def delete_package_and_resources(self, package):
        logging.info(f"No data-catalog found for '{package['name']}', purging package and resources")

        # Delete package resources
        for resource in package.get('resources', []):
            try:
                self.host.action.resource_delete(id=resource['id'])
            except NotFound:
                pass
            except Exception as e:  # An exception occurred
                logging.error(f"Exception occurred while deleting resource '{resource['name']}': {e}")

        # Purging package
        try:
            self.host.action.dataset_purge(id=package['id'])
        except NotFound:
            pass
        except Exception as e:  # An exception occurred
            logging.error(f"Exception occurred while deleting package '{package['name']}': {e}")


def check_catalog_existence(request):
    logging.info("Initialized function")

    if 'CKAN_API_KEY' in os.environ and 'CKAN_SITE_URL' in os.environ and 'GITHUB_API_KEY' in os.environ:
        CKANProcessor().process()
    else:
        logging.error('Function has insufficient configuration')


if __name__ == '__main__':
    check_catalog_existence(None)
