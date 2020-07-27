"""
Creates analytics.movie Bigquery table
"""

from google.cloud import bigquery

client = bigquery.Client()
table_id = "edgart-experiments.analytics.movie"

job_config = bigquery.QueryJobConfig(destination=table_id)

sql = """
SELECT
  imdb.tconst,
  imdb.primaryTitle AS primary_title,
  imdb.originalTitle AS original_title,
  imdb.startYear AS start_year,
  imdb.endYear AS end_year,
  imdb.runtimeMinutes AS runtime_minutes,
  tmdb.budget,
  tmdb.homepage,
  tmdb.original_language,
  tmdb.overview,
  tmdb.popularity,
  tmdb.poster_path,
  tmdb.production_companies,
  tmdb.production_countries,
  tmdb.release_date,
  tmdb.revenue,
  tmdb.runtime,
  tmdb.spoken_languages,
  tmdb.status,
  tmdb.tagline,
  tmdb.vote_average,
  tmdb.vote_count
FROM
  `edgart-experiments.imdb.title_basics` imdb
LEFT JOIN
  `edgart-experiments.tmdb.movies` tmdb
ON
  tmdb.imdb_id = imdb.tconst
WHERE
  isAdult = FALSE
  AND titleType IN ('short',
    'movie',
    'tvMovie')
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))