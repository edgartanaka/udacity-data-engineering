from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_bq import StageBigqueryOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from helpers import SqlQueries
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator

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

###########################
# Stage TMDB
###########################
TMDB_MOVIE_SCHEMA = [
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

stage_tmdb_movies = StageBigqueryOperator(
    storage_uri='gs://udacity-de/tmdb/*.json',
    table_id='tmdb.movies',
    schema=TMDB_MOVIE_SCHEMA,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    task_id='stage_tmdb_movies',
    dag=dag)

###########################
# Stage Awards
###########################
stage_oscars = StageBigqueryOperator(
    storage_uri='gs://udacity-de/awards/oscars.csv',
    table_id='awards.oscars',
    schema=[
        bigquery.SchemaField("year_filme", "INT64"),
        bigquery.SchemaField("year_ceremony", "INT64"),
        bigquery.SchemaField("ceremony", "INT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("film", "STRING"),
        bigquery.SchemaField("winner", "BOOLEAN"),
    ],
    task_id='stage_oscars',
    dag=dag)

stage_bafta = StageBigqueryOperator(
    storage_uri='gs://udacity-de/awards/bafta.csv',
    table_id='awards.bafta',
    schema=[
        bigquery.SchemaField("year", "INT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("nominee", "STRING"),
        bigquery.SchemaField("workers", "STRING"),
        bigquery.SchemaField("winner", "BOOLEAN"),
    ],
    task_id='stage_bafta',
    dag=dag)

stage_golden_globe = StageBigqueryOperator(
    storage_uri='gs://udacity-de/awards/goldenglobe.csv',
    table_id='awards.golden_globe',
    schema=[
        bigquery.SchemaField("year_film", "INT64"),
        bigquery.SchemaField("year_award", "INT64"),
        bigquery.SchemaField("ceremony", "INT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("nominee", "STRING"),
        bigquery.SchemaField("film", "STRING"),
        bigquery.SchemaField("win", "BOOLEAN"),
    ],
    task_id='stage_golden_globe',
    dag=dag)

stage_saga = StageBigqueryOperator(
    storage_uri='gs://udacity-de/awards/screen_actor_guild_awards.csv',
    table_id='awards.saga',
    schema=[
        bigquery.SchemaField("year", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("full_name", "STRING"),
        bigquery.SchemaField("show", "STRING"),
        bigquery.SchemaField("won", "BOOLEAN"),
    ],
    task_id='stage_oscars',
    dag=dag)

###########################
# Create Analytics tables
###########################
analytics_movie = BigQueryOperator(
    sql=SqlQueries.analytics_movie_insert,
    destination_dataset_table='analytics.movie',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_movie',
    dag=dag
)

analytics_person = BigQueryOperator(
    sql=SqlQueries.analytics_person_insert,
    destination_dataset_table='analytics.person',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_person',
    dag=dag
)

analytics_movie_person = BigQueryOperator(
    sql=SqlQueries.analytics_movie_person_insert,
    destination_dataset_table='analytics.movie_person',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_movie_person',
    dag=dag
)

analytics_genre = BigQueryOperator(
    sql=SqlQueries.analytics_genre_insert,
    destination_dataset_table='analytics.genre',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_genre',
    dag=dag
)

analytics_tag = BigQueryOperator(
    sql=SqlQueries.analytics_tag_insert,
    destination_dataset_table='analytics.tag',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_tag',
    dag=dag
)

analytics_rating = BigQueryOperator(
    sql=SqlQueries.analytics_rating_insert,
    destination_dataset_table='analytics.genre',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_rating',
    dag=dag
)

analytics_production_company = BigQueryOperator(
    sql=SqlQueries.analytics_production_company_insert,
    destination_dataset_table='analytics.production_company',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_production_company',
    dag=dag
)

analytics_award_oscar = BigQueryOperator(
    sql=SqlQueries.analytics_award_oscar_insert,
    destination_dataset_table='analytics.award',
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_award_oscar',
    dag=dag
)

analytics_award_golden_globe = BigQueryOperator(
    sql=SqlQueries.analytics_award_golden_globe_insert,
    destination_dataset_table='analytics.award',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_award_golden_globe',
    dag=dag
)

analytics_award_saga = BigQueryOperator(
    sql=SqlQueries.analytics_award_saga_insert,
    destination_dataset_table='analytics.award',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    task_id='analytics_award_saga',
    dag=dag
)

drop_awards = BigQueryTableDeleteOperator(
    deletion_dataset_table='analytics.award',
    ignore_if_missing=True,
    task_id='deletion_dataset_table',
    dag=dag
)

###########################
# Key stages tasks
###########################
staging_complete = DummyOperator(task_id='staging_complete', dag=dag)
analytics_complete = DummyOperator(task_id='analytics_complete', dag=dag)
validation_complete = DummyOperator(task_id='analytics_complete', dag=dag)
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
start_operator >> stage_tmdb_movies >> staging_complete

# Stage all Awards data
start_operator >> stage_oscars >> staging_complete
start_operator >> stage_saga >> staging_complete
start_operator >> stage_bafta >> staging_complete
start_operator >> stage_golden_globe >> staging_complete

# Stage all Movielens data
start_operator >> stage_ml_links >> staging_complete
start_operator >> stage_ml_movies >> staging_complete
start_operator >> stage_ml_ratings >> staging_complete
start_operator >> stage_ml_genome_tags >> staging_complete
start_operator >> stage_ml_genome_scores >> staging_complete

# Create Analytics tables
staging_complete >> [analytics_movie, analytics_person]
analytics_movie >> [analytics_rating, analytics_tag, analytics_genre, analytics_production_company] >> analytics_complete
[analytics_movie, analytics_person] >> analytics_movie_person
analytics_movie_person >> drop_awards
drop_awards >> [analytics_award_saga,analytics_award_oscar,analytics_award_golden_globe] >> analytics_complete

# analytics_complete >> validation_complete

analytics_complete >> end_operator
