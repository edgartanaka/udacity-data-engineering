from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_bq import StageBigqueryOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery

default_args = {
    'owner': 'edgart',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False,
    'scheduling_interval': None
}

dag = DAG('movie_analytics_dag',
          default_args=default_args,
          description='Build Movie Analytics database',
          schedule_interval=None)

start_operator = DummyOperator(task_id='start_dag', dag=dag)

stage_imdb_name_basics = StageBigqueryOperator(
    storage_uri='gs://udacity-de/imdb/name.basics.tsv.gz',
    table_id='imdb.name_basics',
    schema=[
        bigquery.SchemaField("nconst", "STRING"),
        bigquery.SchemaField("primaryName", "STRING"),
        bigquery.SchemaField("birthYear", "STRING"),
        bigquery.SchemaField("deathYear", "STRING"),
        bigquery.SchemaField("primaryProfession", "STRING"),
        bigquery.SchemaField("knownForTitles", "STRING"),
    ],
    field_delimiter='\t',
    null_marker='\\N',
    quote_character='^',
    task_id='stage_imdb_name_basics',
    dag=dag)

stage_imdb_title_basics = StageBigqueryOperator(
    storage_uri='gs://udacity-de/imdb/title.basics.tsv.gz',
    table_id='imdb.title_basics',
    schema=[
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("titleType", "STRING"),
        bigquery.SchemaField("primaryTitle", "STRING"),
        bigquery.SchemaField("originalTitle", "STRING"),
        bigquery.SchemaField("isAdult", "BOOLEAN"),
        bigquery.SchemaField("startYear", "STRING"),
        bigquery.SchemaField("endYear", "STRING"),
        bigquery.SchemaField("runtimeMinutes", "INT64"),
        bigquery.SchemaField("genres", "STRING")
    ],
    field_delimiter='\t',
    null_marker='\\N',
    quote_character='^',
    task_id='stage_imdb_title_basics',
    dag=dag)

stage_imdb_title_ratings = StageBigqueryOperator(
    storage_uri='gs://udacity-de/imdb/title.ratings.tsv.gz',
    table_id='imdb.title_ratings',
    schema=[
        bigquery.SchemaField("nconst", "STRING"),
        bigquery.SchemaField("averageRating", "NUMERIC"),
        bigquery.SchemaField("numVotes", "INT64"),
    ],
    field_delimiter='\t',
    null_marker='\\N',
    quote_character='^',
    task_id='stage_imdb_title_ratings',
    dag=dag)

stage_imdb_title_principals = StageBigqueryOperator(
    storage_uri='gs://udacity-de/imdb/title.principals.tsv.gz',
    table_id='imdb.title_principals',
    schema=[
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("ordering", "INT64"),
        bigquery.SchemaField("nconst", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("job", "STRING"),
        bigquery.SchemaField("characters", "STRING"),
    ],
    field_delimiter='\t',
    null_marker='\\N',
    quote_character='^',
    task_id='stage_imdb_title_principals',
    dag=dag)

stage_imdb_title_crew = StageBigqueryOperator(
    storage_uri='gs://udacity-de/imdb/title.crew.tsv.gz',
    table_id='imdb.title_crew',
    schema=[
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("directors", "STRING"),
        bigquery.SchemaField("writers", "STRING"),
    ],
    field_delimiter='\t',
    null_marker='\\N',
    quote_character='^',
    task_id='stage_imdb_title_crew',
    dag=dag)

###########################
# Movielens tasks
###########################
stage_ml_genome_tags = StageBigqueryOperator(
    storage_uri='gs://udacity-de/ml-25m/genome-tags.csv',
    table_id='ml.genome_tags',
    schema=[
        bigquery.SchemaField("tagId", "INT64"),
        bigquery.SchemaField("tag", "STRING"),
    ],
    task_id='stage_ml_genome_tags',
    dag=dag)

stage_ml_genome_scores = StageBigqueryOperator(
    storage_uri='gs://udacity-de/ml-25m/genome-scores.csv',
    table_id='ml.genome_scores',
    schema=[
        bigquery.SchemaField("movieId", "INT64"),
        bigquery.SchemaField("tagId", "INT64"),
        bigquery.SchemaField("relevance", "FLOAT64"),
    ],
    task_id='stage_ml_genome_scores',
    dag=dag)

stage_ml_movies = StageBigqueryOperator(
    storage_uri='gs://udacity-de/ml-25m/movies.csv',
    table_id='ml.movies',
    schema=[
        bigquery.SchemaField("movieId", "INT64"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("genres", "STRING"),
    ],
    task_id='stage_ml_movies',
    dag=dag)

stage_ml_links = StageBigqueryOperator(
    storage_uri='gs://udacity-de/ml-25m/links.csv',
    table_id='ml.links',
    schema=[
        bigquery.SchemaField("movieId", "INT64"),
        bigquery.SchemaField("imdbId", "INT64"),
        bigquery.SchemaField("tmdbId", "INT64"),
    ],
    task_id='stage_ml_links',
    dag=dag)

stage_ml_ratings = StageBigqueryOperator(
    storage_uri='gs://udacity-de/ml-25m/ratings.csv',
    table_id='ml.ratings',
    schema=[
        bigquery.SchemaField("userId", "INT64"),
        bigquery.SchemaField("movieId", "INT64"),
        bigquery.SchemaField("rating", "NUMERIC"),
        bigquery.SchemaField("timestamp", "INT64"),
    ],
    task_id='stage_ml_ratings',
    dag=dag)

staging_complete = DummyOperator(task_id='staging_complete', dag=dag)

end_operator = DummyOperator(task_id='finished_dag', dag=dag)

###########################
# Tasks Dependencies
###########################

# Stage all IMDB data
start_operator >> stage_imdb_name_basics >> staging_complete
start_operator >> stage_imdb_title_crew >> staging_complete
start_operator >> stage_imdb_title_ratings >> staging_complete
start_operator >> stage_imdb_title_principals >> staging_complete
start_operator >> stage_imdb_title_basics >> staging_complete

# Stage all TMDB data


# Stage all Movielens data
start_operator >> stage_ml_links >> staging_complete
start_operator >> stage_ml_movies >> staging_complete
start_operator >> stage_ml_ratings >> staging_complete
start_operator >> stage_ml_genome_tags >> staging_complete
start_operator >> stage_ml_genome_scores >> staging_complete

# Stage all Awards data


staging_complete >> end_operator
