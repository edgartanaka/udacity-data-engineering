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
    "artist" varchar(2048) NULL,
    "auth" varchar(2048) NOT NULL,
    "firstName" varchar(2048) NULL,
    "gender" varchar(1) NULL,
    "itemInSession" int NOT NULL,
    "lastName" varchar(2048) NULL,
    "length" numeric(16,8) NULL,
    "level" varchar(16) NULL,
    "location" varchar(2048) NULL,
    "method" varchar(8) NOT NULL,
    "page" varchar(16) NOT NULL,
    "registration" numeric(16,1) NULL,
    "sessionId" int NULL,
    "song" varchar(2048) NULL,
    "status" int NULL,
    "ts" bigint NULL,
    "userAgent" varchar(2048) NULL,
    "userId" int NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE "stg_songs" (
    "num_songs" int NULL,
    "artist_id" varchar(256) NULL,
    "artist_latitude" numeric(8,2) NULL,
    "artist_longitude" numeric(8,2) NULL,
    "artist_location" varchar(2048) NULL,
    "artist_name" varchar(2048) NULL,
    "song_id" varchar(256) NULL,
    "title" varchar(2048) NULL,
    "duration" numeric(16, 8) NULL,
    "year" int NULL
)  
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
    "songplay_id" int IDENTITY(0,1) NOT NULL PRIMARY KEY,
    "start_time" TIMESTAMP NOT NULL,
    "user_id" int NOT NULL,
    "level" varchar(16) NULL,
    "song_id" varchar(256) NOT NULL,
    "artist_id" varchar(256) NOT NULL,
    "session_id" int NOT NULL,
    "location" varchar(128) NOT NULL,
    "user_agent" varchar(2048) NOT NULL
)  
""")

user_table_create = ("""
CREATE TABLE "users" (
    "user_id" int UNIQUE NOT NULL PRIMARY KEY,
    "first_name" varchar(2048) NULL,
    "last_name" varchar(2048) NULL,
    "gender" varchar(1) NULL,
    "level" varchar(16) NULL
) 
""")

song_table_create = ("""
CREATE TABLE "songs" (
    "song_id" varchar(256) UNIQUE NOT NULL PRIMARY KEY,
    "artist_id" varchar(256) NOT NULL,
    "title" varchar(2048) NOT NULL,
    "duration" numeric(16, 8) NULL,
    "year" int NOT NULL    
) 
""")

artist_table_create = ("""
CREATE TABLE "artists" (
    "artist_id" varchar(256) UNIQUE NOT NULL PRIMARY KEY,
    "name" varchar(2048) NOT NULL,
    "latitude" numeric(8,2) NULL,
    "longitude" numeric(8,2) NULL,
    "location" varchar(2048) NOT NULL
) 
""")

time_table_create = ("""
CREATE TABLE "times" (
    "start_time" timestamp UNIQUE NOT NULL PRIMARY KEY,
    "hour" int NOT NULL,
    "day" int NOT NULL,
    "week" int NOT NULL,
    "month" int NOT NULL,
    "year" int NOT NULL,
    "weekday" varchar(16) NOT NULL
) 
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stg_events from  {} 
    iam_role {}
    region 'us-west-2'
    FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy stg_songs FROM  {} 
    iam_role {}
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT  
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time,
        userId AS user_id,
        level,
        s.song_id,
        s.artist_id,
        sessionId AS session_id,
        location,
        userAgent AS user_agent
     FROM  stg_events ev     
     JOIN  stg_songs s ON (ev.song=s.title and ev.artist=s.artist_name)
     WHERE  ev.page='NextSong' 
    );     
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    (SELECT DISTINCT  
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
     FROM stg_events
     WHERE page='NextSong'  
        and userId is not null);
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, artist_id, title, duration, year)
    (SELECT DISTINCT  
        song_id,
        artist_id,
        title,
        duration,
        year
     FROM  stg_songs);
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, longitude, latitude, location)
    (SELECT DISTINCT  
        artist_id,
        artist_name AS name,
        artist_longitude AS longitude,
        artist_latitude AS latitude,
        artist_location AS location
     FROM  stg_songs);
""")

time_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    (SELECT DISTINCT 
        start_time,
        EXTRACT(hour FROM  start_time) AS hour,
        EXTRACT(day FROM  start_time) AS day,
        EXTRACT(week FROM  start_time) AS week,
        EXTRACT(month FROM  start_time) AS month,
        EXTRACT(year FROM  start_time) AS year,
        EXTRACT(weekday FROM  start_time) AS weekday
    FROM  songplays)
""")

# QUERY LISTS

# original
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



