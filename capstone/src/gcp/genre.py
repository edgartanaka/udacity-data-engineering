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
table_id = "{}.analytics.genre".format(credentials.project_id)

job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition='WRITE_TRUNCATE',)

sql = """
WITH imdb_genres AS (
    SELECT 
      tconst, -- string
      genre
    FROM `edgart-experiments.imdb.title_basics`,
    UNNEST(split(lower(genres), ',')) as genre
),

ml_genres AS (
    SELECT 
    l.imdbId as tconst, -- imdbId is an integer
    genre
    FROM `edgart-experiments.ml.movies` m,
    UNNEST(split(lower(m.genres), '|')) as genre
    JOIN `edgart-experiments.ml.links` l on l.movieId=m.movieId
    WHERE genre != '(no genres listed)'
),

ml_genres_with_tconst AS (
    SELECT m.tconst, ml.genre
    FROM ml_genres ml 
    JOIN `edgart-experiments.analytics.movie` m -- join only movies (ignore series and others)
    ON CAST(REPLACE(m.tconst, 'tt', '') AS INT64)=ml.tconst -- join based on integer
),
all_genres_with_duplicates AS (
    SELECT * FROM ml_genres_with_tconst
    UNION ALL
    SELECT * FROM imdb_genres
),
all_genres_without_duplicates AS (
    SELECT DISTINCT tconst, genre 
    FROM all_genres_with_duplicates
)

SELECT tconst, genre
FROM all_genres_without_duplicates
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))