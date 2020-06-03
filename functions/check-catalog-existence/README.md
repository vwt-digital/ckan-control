# Check data-catalog existence
Cloud function to check the existence of data-catalog files for CKAN packages.

## Setup
This function needs three environment variables to work:
- `API_KEY`: The CKAN API key
- `CKAN_SITE_URL`: The CKAN API site url
- `GITHUB_API_KEY`: The GitHub API key to access public and private repositories

## Function
The check-catalog-existence works as follows:
1. With help of the [CKAN API](https://docs.ckan.org/en/ckan-2.7.3/api/) the function will list all current packages;
2. For each package a request will be done to receive it's information and check if the variable `github_url` is present;
3. If this variable is existing it will use this url to check if the file existing with help of the [GITHUB API](https://developer.github.com/v3/);
4. If this file is not existing, the function will first delete all package's resources and then purge the package.

## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License
