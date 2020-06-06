import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE "stg_events" (
    "artist" character varying(256) NULL,
    "auth" character varying(256) NOT NULL,
    "firstName" character varying(256) NULL,
    "gender" character varying(1) NULL,
    "itemInSession" int NOT NULL,
    "lastName" character varying(256) NULL,
    "length" numeric(16,8) NULL,
    "level" character varying(16) NULL,
    "location" character varying(128) NULL,
    "method" character varying(8) NOT NULL,
    "page" character varying(16) NOT NULL,
    "registration" numeric(16,1) NULL,
    "sessionId" int NOT NULL,
    "song" character varying(256) NULL,
    "status" int NULL,
    "ts" bigint NOT NULL,
    "userAgent" character varying(256) NULL,
    "userId" int NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE "stg_songs" (
    "num_songs" int NOT NULL,
    "artist_id" int NOT NULL,
    "artist_latitude" numeric(8,2) NULL,
    "artist_longitude" numeric(8,2) NULL,
    "artist_location" character varying(256) NOT NULL,
    "artist_name" character varying(256) NOT NULL,
    "song_id" character varying(128) NOT NULL,
    "title" character varying(256) NOT NULL,
    "duration" numeric(16, 8) NULL,
    "year" int NULL
)  
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
    "songplay_id" int IDENTITY(0,1) NOT NULL,
    "start_time" TIMESTAMP NOT NULL,
    "user_id" int NOT NULL,
    "level" character varying(16) NOT NULL,
    "song_id" character varying(256) NOT NULL,
    "artist_id" int NOT NULL,
    "session_id" int NOT NULL,
    "location" character varying(128) NOT NULL,
    "user_agent" character varying(256) NOT NULL
)  
""")

user_table_create = ("""
CREATE TABLE "users" (
    "user_id" int IDENTITY(0,1) NOT NULL,
    "first_name" character varying(256) NOT NULL,
    "last_name" character varying(256) NOT NULL,
    "gender" character varying(1) NOT NULL,
    "level" character varying(16) NOT NULL
) 
""")

song_table_create = ("""
CREATE TABLE "songs" (
    "song_id" int IDENTITY(0,1) NOT NULL,
    "artist_id" int NOT NULL,
    "title" character varying(256) NOT NULL,
    "duration" numeric(16, 8) NULL,
    "year" int NOT NULL    
) 
""")

artist_table_create = ("""
CREATE TABLE "artists" (
    "artist_id" int IDENTITY(0,1) NOT NULL,
    "name" character varying(256) NOT NULL,
    "latitude" numeric(8,2) NULL,
    "longitude" numeric(8,2) NULL,
    "location" character varying(256) NOT NULL
) 
""")

time_table_create = ("""
CREATE TABLE "times" (
    "start_time" int UNIQUE NOT NULL,
    "hour" int NOT NULL,
    "day" int NOT NULL,
    "week" int NOT NULL,
    "month" int NOT NULL,
    "year" int NOT NULL,
    "weekday" character varying(16) NOT NULL
) 
""")

# STAGING TABLES

# LOG_DATA='s3://udacity-dend/log-data'
# LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
# SONG_DATA='s3://udacity-dend/song-data'

staging_events_copy = ("""
    copy stg_events from {} 
    iam_role {}
    FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy stg_songs from {} 
    iam_role {}
    json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
