# Check data-catalog existence
Function to check the existence of CKAN package's resources on the Google Cloud Platform.

## Setup
1. Make sure a ```config.py``` file exists within the directory with the correct configuration:
    ~~~
    DELEGATED_SA = The GCP Service Account with all necessary rights to check resources
    JIRA_ACTIVE = Boolean to enabled or disable creating JIRA issues for non-existing issues
    JIRA_USER = User account for JIRA API
    JIRA_API_DOMAIN = URL towards JIRA domain
    JIRA_PROJECT_ID = JIRA project ID for to-be-created issues
    JIRA_ISSUE_TYPE_ID = JIRA issue type ID for to-be-created issues
    JIRA_BOARD_ID = JIRA board ID for retrieving current sprint for to-be-created issues
    JIRA_EPIC = Epic name for to-be-created issues
    ~~~
2. Make sure the following variables are present in the environment:
    ~~~
    CKAN_API_KEY = The CKAN API key to access the database
    CKAN_SITE_URL = The host URL of CKAN
    JIRA_API_KEY = The JIRA API key to create issues for non-existing resources
    ~~~
3. Create a custom Google Cloud Platform role and assign this to the delegated service account (see [Permissions](#permissions));
4. Deploy the function with help of the [cloudbuild.example.yaml](cloudbuild.example.yaml) to the Google Cloud Platform.

## Function
The check-catalog-existence works as follows:
1. With help of the [CKAN API](https://docs.ckan.org/en/ckan-2.7.3/api/) the function will list all current packages;
2. Each package's resources will be checked to make sure the resource is still existing;
3. If a resource is not existing anymore, the function will raise a notification with the correct information.

## Permissions
This function depends on a Service Account (hereafter SA) with specific permissions to access project resources. Because the pre-defined roles within the platform doesn't suit our needs, 
a custom role has to be defined and assigned to the SA. To create a custom role within GCP you can follow [this guide](https://cloud.google.com/iam/docs/creating-custom-roles). 
The custom role must have the following permissions:
- `bigquery.datasets.get`: Getting all BigQuery databases in a project
- `cloudsql.databases.list`: Listing all Cloud SQL instance databases in a project
- `cloudsql.instances.list`: Listing all Cloud SQL instances in a project
- `pubsub.subscriptions.list`: Listing all Pub/Sub subscriptions in a project
- `pubsub.topics.list`: Listing all Pub/Sub topics in a project
- `serviceusage.services.list`: Listing all enabled services in a project
- `storage.buckets.list`: Listing all buckets within a project

After creating this role, assign it to the delegated SA on the highest hierarchy level possible.

## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License
