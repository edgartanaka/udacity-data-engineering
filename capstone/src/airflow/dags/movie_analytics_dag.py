from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageImdbOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateExternalTableOperator, BigQueryDeleteDatasetOperator
from airflow.utils.dates import days_ago

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
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None
          )

start_operator = DummyOperator(task_id='start_dag', dag=dag)

stage_imdb_title_basics = BigQueryCreateExternalTableOperator(
    task_id='stage_imdb_title_basics',
    dag=dag,
    bucket='udacity-de',
    source_objects=['imdb/title.basics.tsv.gz'],
    schema_fields=[
        {
            "mode": "NULLABLE",
            "name": "tconst",
            "type": "STRING"
        },
        {
            "mode": "NULLABLE",
            "name": "titleType",
            "type": "STRING"
        },
        {
            "mode": "NULLABLE",
            "name": "primaryTitle",
            "type": "STRING"
        },
        {
            "mode": "NULLABLE",
            "name": "originalTitle",
            "type": "STRING"
        },
        {
            "mode": "NULLABLE",
            "name": "isAdult",
            "type": "BOOLEAN"
        },
        {
            "mode": "NULLABLE",
            "name": "startYear",
            "type": "STRING"
        },
        {
            "mode": "NULLABLE",
            "name": "endYear",
            "type": "STRING"
        },
        {
            "mode": "NULLABLE",
            "name": "runtimeMinutes",
            "type": "INTEGER"
        },
        {
            "mode": "NULLABLE",
            "name": "genres",
            "type": "STRING"
        }
    ],
    destination_project_dataset_table='edgart-experiments.imdb.title_basics',
    field_delimiter='\t',
    compression='GZIP',
    source_format='CSV',
    skip_leading_rows=1
)

delete_imdb = BigQueryDeleteDatasetOperator(
    dataset_id='imdb',
    delete_contents=True,
    task_id='delete_imdb',
    dag=dag
)

end_operator = DummyOperator(task_id='finished_dag', dag=dag)

###########################
# dependencies
###########################

start_operator >> delete_imdb >> stage_imdb_title_basics >> end_operator
