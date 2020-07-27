"""
Loads data from Storage into Bigquery
Data format is JSON
"""
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict, BadRequest

key_path = "../../credentials/edgart-experiments-67ca4ddbda73.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id, )

# from google.cloud import bigquery
# client = bigquery.Client()
# dataset_id = 'my_dataset'

dataset_ref = client.dataset('tmdb')
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job_config.schema = [
    bigquery.SchemaField("adult", "BOOLEAN"),
    bigquery.SchemaField("backdrop_path", "STRING"),
    bigquery.SchemaField("belongs_to_collection",
                         "RECORD",
                         fields=[
                             bigquery.SchemaField("id", "INT64"),
                             bigquery.SchemaField("name", "STRING"),
                             bigquery.SchemaField("poster_path", "STRING"),
                             bigquery.SchemaField("backdrop_path", "STRING"),
                         ]),
    bigquery.SchemaField("budget", "INT64"),
    bigquery.SchemaField("genres",
                         "RECORD",
                         mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("id", "INT64"),
                             bigquery.SchemaField("name", "STRING"),
                         ]),
    bigquery.SchemaField("homepage", "STRING"),
    bigquery.SchemaField("id", "INT64"),
    bigquery.SchemaField("imdb_id", "STRING"),
    bigquery.SchemaField("original_language", "STRING"),
    bigquery.SchemaField("original_title", "STRING"),
    bigquery.SchemaField("overview", "STRING"),
    bigquery.SchemaField("popularity", "NUMERIC"),
    bigquery.SchemaField("poster_path", "STRING"),
    bigquery.SchemaField("production_companies",
                         "RECORD",
                         mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("id", "INT64"),
                             bigquery.SchemaField("logo_path", "STRING"),
                             bigquery.SchemaField("name", "STRING"),
                             bigquery.SchemaField("origin_country", "STRING"),
                         ]),
    bigquery.SchemaField("production_countries",
                         "RECORD",
                         mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("iso_3166_1", "STRING"),
                             bigquery.SchemaField("name", "STRING"),
                         ]),
    bigquery.SchemaField("release_date", "STRING"),
    bigquery.SchemaField("revenue", "INT64"),
    bigquery.SchemaField("runtime", "INT64"),
    bigquery.SchemaField("spoken_languages",
                         "RECORD",
                         mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("iso_639_1", "STRING"),
                             bigquery.SchemaField("name", "STRING"),
                         ]),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("tagline", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("video", "BOOLEAN"),
    bigquery.SchemaField("vote_average", "NUMERIC"),
    bigquery.SchemaField("vote_count", "INT64"),
]
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
uri = "gs://udacity-de/tmdb/*.json"

load_job = client.load_table_from_uri(
    uri,
    dataset_ref.table("movies"),
    location="US",  # Location must match that of the destination dataset.
    job_config=job_config,
)  # API request
print("Starting job {}".format(load_job.job_id))

try:
    load_job.result()  # Waits for table load to complete.
    print("Job finished.")
except BadRequest as e:
    for e in load_job.errors:
        print('ERROR: {}'.format(e['message']))

destination_table = client.get_table(dataset_ref.table("movies"))
print("Loaded {} rows.".format(destination_table.num_rows))
