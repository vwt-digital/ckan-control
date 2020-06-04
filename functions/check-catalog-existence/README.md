# Check data-catalog existence
Cloud function to check the existence of data-catalog files for CKAN packages.

## Setup
1. Make sure a ```config.py``` file exists within the directory with the correct configuration:
    ~~~
    DELEGATED_SA = The GCP Service Account with all necessary rights to check resources
    ~~~
2. Make sure the following variables are present in the environment:
    ~~~
    API_KEY = The CKAN API key to access the database
    CKAN_SITE_URL = The host URL of CKAN
    ~~~
3. Deploy the function with help of the [cloudbuild.example.yaml](cloudbuild.example.yaml) to the Google Cloud Platform.

## Function
The check-catalog-existence works as follows:
1. With help of the [CKAN API](https://docs.ckan.org/en/ckan-2.7.3/api/) the function will list all current packages;
2. Each package's resources will be checked to make sure the resource is still existing;
3. If a resource is not existing anymore, the function will raise a notification with the correct information.

## Permission
This function depends on a Service Account (hereafter SA) with specific permissions to access project resources. Because the pre-defined roles within the platform doesn't suit our needs, 
a custom role has to be defined and assigned to the SA. To create a custom role within GCP you can follow [this guide](https://cloud.google.com/iam/docs/creating-custom-roles). 
The custom role must have the following permissions:
- `storage.buckets.list`: Listing all buckets within a project
- `serviceusage.services.list`: Listing all enabled services in a project

After creating this role, assign it to the delegated SA on the highest level possible.

## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License
