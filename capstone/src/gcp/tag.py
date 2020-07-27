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
table_id = "{}.analytics.tag".format(credentials.project_id)

job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition='WRITE_TRUNCATE',)

sql = """
SELECT  
  m.tconst,
  gt.tag,
  gs.relevance
FROM `edgart-experiments.ml.genome_scores` gs
JOIN `edgart-experiments.ml.genome_tags` gt ON gt.tagId=gs.tagId
JOIN `edgart-experiments.ml.links` l ON l.movieId=gs.movieId
JOIN `edgart-experiments.analytics.movie` m ON CAST(REPLACE(m.tconst, 'tt', '') AS INT64)=l.imdbId
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))