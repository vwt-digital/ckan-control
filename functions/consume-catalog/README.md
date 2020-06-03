# Consume Data-Catalogs
This function consumes data-catalogs posted on a Pub/Sub Topic and adds them to a CKAN database.

## Setup
1. Make sure a ```config.py``` file exists within the directory, based on the [config.example.py](config.example.py), with the correct configuration:
    ~~~
    DATA_CATALOG_PROPERTIES = Identifiers for the data-catalog routes
    ~~~
2. Make sure the following variables are present in the environment:
    ~~~
    DATA_SELECTOR = The identifier used for this configuration, based on the DATA_CATALOG_PROPERTIES
    API_KEY = The CKAN API key to access the database
    CKAN_SITE_URL = The host URL of CKAN
    ~~~
3. Deploy the function with help of the [cloudbuild.example.yaml](cloudbuild.example.yaml) to the Google Cloud Platform.

## Incoming message
To make sure the function works according to the way it was intented, the incoming messages from a Pub/Sub Topic must have the following structure based on the [company-data structure](https://vwt-digital.github.io/project-company-data.github.io/v1.1/schema):
~~~JSON
{
  "gobits": [ ],
  "data_catalog": {
    "conformsTo": "https://vwt-digital.github.io/project-company-data.github.io/v1.1/schema",
    "projectId": "project-id",
    "backupDestination": "backup-destination",
    "publishDataCatalog": {
      "topic": "data-catalogs-topic",
      "project": "data-catalogs-project"
    },
    "dataset": [ ]
  }
}
~~~

## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License
