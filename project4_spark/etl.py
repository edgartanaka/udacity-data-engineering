import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')
config = config['default']

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']

# input files
S3_PATH = "s3a://udacity-de-edgar/data/"
SONG_FILES_PATH_SUFFIX = 'song_data/*/*/*/*.json'
LOG_FILES_PATH_SUFFIX = 'log_data/*.json'


def create_spark_session():
    """
    Creates a spark session
    :return: a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_songs(spark, input_data, output_data):
    """
    Reads the song dataset and creates the dimension song dataset.
    The output dataset does not contain duplicated songs.
    The output dataset is saved in S3, it is partitioned by year/artist_id, it is in Parquet format
    and it overwrites the previous version of the dataset in S3 whenever this function is called.
    :param spark: spark session
    :param input_data: path to the input files
    :param output_data: path to the output files
    :return: None
    """
    # read song data file
    df = spark.read.json(input_data + SONG_FILES_PATH_SUFFIX)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
        .partitionBy('year', 'artist_id')\
        .mode('overwrite')\
        .parquet(output_data + 'out_songs/songs.parquet')


def process_artists(spark, input_data, output_data):
    """
    Reads the songs dataset and generates the artists dimension dataset.
    The output dataset does not contain duplicated artists.
    The output dataset is saved in S3, it is in Parquet format
    and it overwrites the previous version of the dataset in S3 whenever this function is called.
    :param spark: spark session
    :param input_data: path to the input files
    :param output_data: path to the output files
    :return: None
    """
    # read song data file
    df = spark.read.json(input_data + SONG_FILES_PATH_SUFFIX)

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                              'artist_longitude').distinct()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'out_artists/artists.parquet')


def process_users(spark, input_data, output_data):
    """
    Reads the log dataset and generates the users dimension dataset.
    The output dataset does not contain duplicated users.
    The output dataset is saved in S3, it is in Parquet format
    and it overwrites the previous version of the dataset in S3 whenever this function is called.
    :param spark: spark session
    :param input_data: path to the input files
    :param output_data: path to the output files
    :return: None
    """
    # read log data file
    df = spark.read.json(input_data + LOG_FILES_PATH_SUFFIX)

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'out_users/users.parquet')


def process_times(spark, input_data, output_data):
    """
    Reads the log dataset and generates the times dimension dataset.
    The output dataset does not contain duplicated times.
    The output dataset is saved in S3, it is partitioned by year/month, it is in Parquet format
    and it overwrites the previous version of the dataset in S3 whenever this function is called.
    :param spark: spark session
    :param input_data: path to the input files
    :param output_data: path to the output files
    :return: None
    """
    # read song data file
    df = spark.read.json(input_data + LOG_FILES_PATH_SUFFIX)

    df = df.select('ts').distinct()

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda z: datetime.utcfromtimestamp(float(z) / 1000.0), TimestampType())
    df = df.withColumn('datetime', get_timestamp('ts'))

    # extract columns to create time table
    time_table = df.select(col("ts").alias('start_time'),
                           hour(col("datetime")).alias("hour"),
                           dayofmonth(col("datetime")).alias("day"),
                           weekofyear(col("datetime")).alias("week"),
                           month(col("datetime")).alias("month"),
                           year(col("datetime")).alias("year"),
                           date_format(col("datetime"), "EEEE").alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'out_times/times.parquet')


def process_songplays(spark, input_data, output_data):
    """
    Reads the log dataset and generates the songplays fact dataset.
    Each record in the output dataset is an user event in the music streaming app.
    The output dataset is saved in S3, it is partitioned by year/month, it is in Parquet format
    and it overwrites the previous version of the dataset in S3 whenever this function is called.
    :param spark: spark session
    :param input_data: path to the input files
    :param output_data: path to the output files
    :return: None
    """
    # read log data file
    log_df = spark.read.json(input_data + LOG_FILES_PATH_SUFFIX)
    song_df = spark.read.json(input_data + SONG_FILES_PATH_SUFFIX)

    # filter only song plays actions
    log_df = log_df.filter(col("page") == 'NextSong')

    # join with songs table
    df = log_df.join(song_df,
                     (song_df['title'] == log_df['song']) & (song_df['artist_name'] == log_df['artist']))
    df = df.select(log_df['*'], song_df['song_id'], song_df['artist_id'])

    # extract datetime from ts
    get_timestamp = udf(lambda z: datetime.utcfromtimestamp(float(z) / 1000.0), TimestampType())
    df = df.withColumn('datetime', get_timestamp('ts'))

    df = df.withColumn("songplay_id", monotonically_increasing_id())
    df = df.withColumn("year", year(col('datetime')))
    df = df.withColumn("month", month(col('datetime')))

    songplays_table = df.select(
        col('songplay_id'),
        col('ts').alias('start_time'),
        year(col('datetime')).alias('year'),
        month(col('datetime')).alias('month'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table \
        .write \
        .mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(output_data + 'out_songplays/songplays.parquet')


def process_song_data(spark, input_data, output_data):
    process_songs(spark, input_data, output_data)
    process_artists(spark, input_data, output_data)


def process_log_data(spark, input_data, output_data):
    process_users(spark, input_data, output_data)
    process_times(spark, input_data, output_data)
    process_songplays(spark, input_data, output_data)


def main():
    spark = create_spark_session()
    input_data = S3_PATH
    output_data = S3_PATH

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
