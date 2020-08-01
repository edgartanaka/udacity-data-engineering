from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict, BadRequest

key_path = "../../credentials/edgart-experiments-67ca4ddbda73.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id, )

def load_csv(uri, table_id, schema):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition='WRITE_TRUNCATE', # if table already exists, drops and recreates
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    try:
        load_job.result()
    except BadRequest as e:
        for e in load_job.errors:
            print('ERROR: {}'.format(e['message']))

    table = client.get_table(table_id)
    print("Loaded {} rows to table {}".format(table.num_rows, table_id))


def stage_ml_genome_tags():
    uri = 'gs://udacity-de/ml-25m/genome-tags.csv'
    table_id = 'ml.genome_tags'
    schema = [
        bigquery.SchemaField("tagId", "INT64"),
        bigquery.SchemaField("tag", "STRING"),
    ]
    load_csv(uri, table_id, schema)


def stage_ml_genome_scores():
    uri = 'gs://udacity-de/ml-25m/genome-scores.csv'
    table_id = 'ml.genome_scores'
    schema = [
        bigquery.SchemaField("movieId", "INT64"),
        bigquery.SchemaField("tagId", "INT64"),
        bigquery.SchemaField("relevance", "FLOAT64"),
    ]
    load_csv(uri, table_id, schema)


def stage_ml_movies():
    uri = 'gs://udacity-de/ml-25m/movies.csv'
    table_id = 'ml.movies'
    schema = [
        bigquery.SchemaField("movieId", "INT64"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("genres", "STRING"),
    ]
    load_csv(uri, table_id, schema)


def stage_ml_links():
    uri = 'gs://udacity-de/ml-25m/links.csv'
    table_id = 'ml.links'
    schema = [
        bigquery.SchemaField("movieId", "INT64"),
        bigquery.SchemaField("imdbId", "INT64"),
        bigquery.SchemaField("tmdbId", "INT64"),
    ]
    load_csv(uri, table_id, schema)


def stage_ml_ratings():
    uri = 'gs://udacity-de/ml-25m/ratings.csv'
    table_id = 'ml.ratings'
    schema = [
        bigquery.SchemaField("userId", "INT64"),
        bigquery.SchemaField("movieId", "INT64"),
        bigquery.SchemaField("rating", "NUMERIC"),
        bigquery.SchemaField("timestamp", "INT64"),
    ]
    load_csv(uri, table_id, schema)


def main():
    stage_ml_links()
    stage_ml_movies()
    stage_ml_ratings()
    stage_ml_genome_tags()
    stage_ml_genome_scores()


if __name__ == "__main__":
    main()
