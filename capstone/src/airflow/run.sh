# THIS IS NOT MEANT TO BE RUN!
# copy and paste the parts you need

# test
airflow run -f -I -i movie_analytics_dag imdb_title_principals 2020-08-01

# test
airflow backfill -t imdb_title_principals --reset_dagruns -i -I -y -l -v -s 2020-01-01 movie_analytics_dag

# set home
export AIRFLOW_HOME=/Users/edgart/git/udacity-data-engineering/capstone/src/airflow

# start server
airflow webserver -p 8080

# start scheduler
airflow scheduler