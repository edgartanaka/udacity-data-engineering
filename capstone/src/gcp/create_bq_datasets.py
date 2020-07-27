from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict, BadRequest

key_path = "../../credentials/edgart-experiments-67ca4ddbda73.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id, )


def create_dataset(dataset_name):
    dataset_id = "{}.{}".format(client.project, dataset_name)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    try:
        # Send the dataset to the API for creation.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = client.create_dataset(dataset)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except Conflict:
        print('Dataset already exists: {}'.format(dataset_id))


def main():
    create_dataset('imdb')  # staging for IMDB data
    create_dataset('ml')  # staging for movielens data
    create_dataset('tmdb')  # staging for TMDB data
    create_dataset('analytics')  # final analytics tables for end user


if __name__ == "__main__":
    main()
