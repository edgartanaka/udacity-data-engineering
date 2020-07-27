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


def stage_oscars():
    uri = 'gs://udacity-de/awards/oscars.csv'
    table_id = 'awards.oscars'
    schema = [
        bigquery.SchemaField("year_filme", "INT64"),
        bigquery.SchemaField("year_ceremony", "INT64"),
        bigquery.SchemaField("ceremony", "INT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("film", "STRING"),
        bigquery.SchemaField("winner", "BOOLEAN"),
    ]
    load_csv(uri, table_id, schema)


def stage_bafta():
    uri = 'gs://udacity-de/awards/bafta.csv'
    table_id = 'awards.bafta'
    schema = [
        bigquery.SchemaField("year", "INT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("nominee", "STRING"),
        bigquery.SchemaField("workers", "STRING"),
        bigquery.SchemaField("winner", "BOOLEAN"),
    ]
    load_csv(uri, table_id, schema)


def stage_golden_globe():
    uri = 'gs://udacity-de/awards/goldenglobe.csv'
    table_id = 'awards.golden_globe'
    schema = [
        bigquery.SchemaField("year_film", "INT64"),
        bigquery.SchemaField("year_award", "INT64"),
        bigquery.SchemaField("ceremony", "INT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("nominee", "STRING"),
        bigquery.SchemaField("film", "STRING"),
        bigquery.SchemaField("win", "BOOLEAN"),
    ]
    load_csv(uri, table_id, schema)

def stage_saga():
    uri = 'gs://udacity-de/awards/screen_actor_guild_awards.csv'
    table_id = 'awards.saga'
    schema = [
        bigquery.SchemaField("year", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("full_name", "STRING"),
        bigquery.SchemaField("show", "STRING"),
        bigquery.SchemaField("won", "BOOLEAN"),
    ]
    load_csv(uri, table_id, schema)

def main():
    stage_oscars()
    stage_saga()
    stage_bafta()
    stage_golden_globe()


if __name__ == "__main__":
    main()
