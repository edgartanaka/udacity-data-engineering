import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')
config = config['default']

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_songs(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(
        output_data + 'out_songs/songs.parquet')

def process_artists(spark, input_data, output_data):
    song_data = input_data + 'song_data/A/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                              'artist_longitude').distinct()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'out_artists/artists.parquet')

def process_users(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where("page='NextSong'")

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'out_users/users.parquet')


def process_times(spark, input_data, output_data):
    log_data = input_data + 'log_data/*.json'

    # read song data file
    df = spark.read.json(log_data)
    df = df.filter(col("page") == 'NextSong')
    df = df.select('ts').distinct()

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda z: datetime.utcfromtimestamp(float(z)/1000.0), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # extract columns to create time table
    time_table = df.select(col("start_time"),
                           hour(col("start_time")).alias("hour"),
                           dayofmonth(col("start_time")).alias("day"),
                           weekofyear(col("start_time")).alias("week"),
                           month(col("start_time")).alias("month"),
                           year(col("start_time")).alias("year"),
                           date_format(col("start_time"), "EEEE").alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + 'out_times/times.parquet')

    time_table.coalesce(1).write.mode('overwrite').format("com.databricks.spark.csv").option("header", "true").save("times.csv")


def process_songplays(spark, input_data, output_data):
    # # read in song data to use for songplays table
    # song_df =

    # # extract columns from joined song and log datasets to create songplays table
    # songplays_table =

    # # write songplays table to parquet files partitioned by year and month
    # songplays_table
    pass


def process_song_data(spark, input_data, output_data):
    process_songs(spark, input_data, output_data)
    process_artists(spark, input_data, output_data)


def process_log_data(spark, input_data, output_data):
    # process_users(spark, input_data, output_data)
    process_times(spark, input_data, output_data)
    process_songplays(spark, input_data, output_data)


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-de-edgar/data/"
    # output_data = "s3a://udacity-de-edgar/data/"
    input_data = 'data/'
    output_data = 'data/'
    
    # process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
