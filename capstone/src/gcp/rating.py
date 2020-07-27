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
table_id = "{}.analytics.rating".format(credentials.project_id)

job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition='WRITE_TRUNCATE',)

sql = """
WITH ml_rating AS (
  SELECT
    m.tconst,
    TRUNC(AVG(rating), 1) as rating,
    COUNT(1) as num_votes
  FROM `edgart-experiments.ml.ratings` r
  JOIN `edgart-experiments.ml.links` l ON l.movieId=r.movieId 
  JOIN `edgart-experiments.analytics.movie` m ON CAST(REPLACE(m.tconst, 'tt', '') AS INT64)=l.imdbId
  GROUP BY m.tconst
)
SELECT
m.tconst,
imdb.averageRating as imdb_rating,
imdb.numVotes as imdb_num_votes,
tmdb.vote_average as tmdb_rating,
tmdb.vote_count as tmdb_num_votes,
ml.rating as ml_rating,
ml.num_votes as ml_num_votes
FROM `edgart-experiments.analytics.movie` m
LEFT JOIN `edgart-experiments.imdb.title_ratings` imdb ON imdb.nconst=m.tconst
LEFT JOIN `edgart-experiments.tmdb.movies` tmdb ON tmdb.imdb_id=m.tconst
LEFT JOIN ml_rating ml ON ml.tconst=m.tconst
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))