"""
Creates analytics.movie Bigquery table
"""

from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "../../credentials/edgart-experiments-67ca4ddbda73.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
table_id = "{}.analytics.movie_person".format(credentials.project_id)

job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition='WRITE_TRUNCATE',)

sql = """
SELECT  
  tp.tconst,
  tp.nconst,
  t.primaryTitle as movie_primary_title,
  n.primaryName as person_primary_name,
  tp.ordering,
  tp.category,
  tp.job,
  tp.characters
FROM `edgart-experiments.imdb.title_principals` tp
JOIN `edgart-experiments.imdb.title_basics` t ON t.tconst=tp.tconst
JOIN `edgart-experiments.imdb.name_basics` n ON n.nconst=tp.nconst
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))