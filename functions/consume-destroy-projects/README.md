# Consume Destroy-Projects
This function consumes destroyed projects posted on a Pub/Sub Topic and removes corresponding resources from the CKAN database.

## Setup
1. Make sure the following variables are present in the environment:
    ~~~
    API_KEY = The CKAN API key to access the database
    CKAN_SITE_URL = The host URL of CKAN
    ~~~
2. Deploy the function with help of the [cloudbuild.example.yaml](cloudbuild.example.yaml) to the Google Cloud Platform.

## Incoming message
To make sure the function works according to the way it was intented, the incoming messages from a Pub/Sub Topic must have the following structure:
~~~JSON
{
  "gobits": [ ],
  "destroy_projects": [
    {
      "project_id": "destroyed-project-1"
    },
    {
      "project_id": "destroyed-project-2"
    }
  ]
}
~~~

## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License
