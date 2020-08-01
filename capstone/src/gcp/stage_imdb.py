"""
Loads data from Storage into Bigquery
Data format is CSV or TSV
Supports gzipped data
"""
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict, BadRequest

key_path = "../../credentials/edgart-experiments-67ca4ddbda73.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id, )

def load_csv(uri, table_id, schema, field_delimiter=',', null_marker='\\N'):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        field_delimiter=field_delimiter,
        source_format=bigquery.SourceFormat.CSV,
        quote_character='^', # this trick is needed because IMDB's TSV has unescaped quotes
        null_marker=null_marker,
        write_disposition='WRITE_TRUNCATE',
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


def stage_imdb_name_basics():
    uri = 'gs://udacity-de/imdb/name.basics.tsv.gz'
    table_id = 'imdb.name_basics'
    schema = [
        bigquery.SchemaField("nconst", "STRING"),
        bigquery.SchemaField("primaryName", "STRING"),
        bigquery.SchemaField("birthYear", "STRING"),
        bigquery.SchemaField("deathYear", "STRING"),
        bigquery.SchemaField("primaryProfession", "STRING"),
        bigquery.SchemaField("knownForTitles", "STRING"),
    ]
    field_delimiter = '\t'
    load_csv(uri, table_id, schema, field_delimiter)


def stage_imdb_title_basics():
    uri = 'gs://udacity-de/imdb/title.basics.tsv.gz'
    table_id = 'imdb.title_basics'
    schema = [
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("titleType", "STRING"),
        bigquery.SchemaField("primaryTitle", "STRING"),
        bigquery.SchemaField("originalTitle", "STRING"),
        bigquery.SchemaField("isAdult", "BOOLEAN"),
        bigquery.SchemaField("startYear", "STRING"),
        bigquery.SchemaField("endYear", "STRING"),
        bigquery.SchemaField("runtimeMinutes", "INT64"),
        bigquery.SchemaField("genres", "STRING")
    ]
    field_delimiter = '\t'
    load_csv(uri, table_id, schema, field_delimiter)


def stage_imdb_title_ratings():
    uri = 'gs://udacity-de/imdb/title.ratings.tsv.gz'
    table_id = 'imdb.title_ratings'
    schema = [
        bigquery.SchemaField("nconst", "STRING"),
        bigquery.SchemaField("averageRating", "NUMERIC"),
        bigquery.SchemaField("numVotes", "INT64"),
    ]
    field_delimiter = '\t'
    load_csv(uri, table_id, schema, field_delimiter)


def stage_imdb_title_principals():
    uri = 'gs://udacity-de/imdb/title.principals.tsv.gz'
    source_format = bigquery.SourceFormat.CSV
    table_id = 'imdb.title_principals'
    schema = [
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("ordering", "INT64"),
        bigquery.SchemaField("nconst", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("job", "STRING"),
        bigquery.SchemaField("characters", "STRING"),
    ]
    field_delimiter = '\t'
    load_csv(uri, table_id, schema, field_delimiter)


def stage_imdb_title_crew():
    uri = 'gs://udacity-de/imdb/title.crew.tsv.gz'
    source_format = bigquery.SourceFormat.CSV
    table_id = 'imdb.title_crew'
    schema = [
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("directors", "STRING"),
        bigquery.SchemaField("writers", "STRING"),
    ]
    field_delimiter = '\t'
    load_csv(uri, table_id, schema, field_delimiter)



def stage_imdb():
    stage_imdb_name_basics()
    stage_imdb_title_basics()
    stage_imdb_title_ratings()
    stage_imdb_title_crew()
    stage_imdb_title_principals()

def main():
    stage_imdb()

if __name__ == "__main__":
    main()
