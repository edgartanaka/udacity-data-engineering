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
    "sessionId" int NOT NULL,
    "song" varchar(2048) NULL,
    "status" int NULL,
    "ts" bigint NOT NULL,
    "userAgent" varchar(2048) NULL,
    "userId" int NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE "stg_songs" (
    "num_songs" int NOT NULL,
    "artist_id" varchar(256) NOT NULL,
    "artist_latitude" numeric(8,2) NULL,
    "artist_longitude" numeric(8,2) NULL,
    "artist_location" varchar(2048) NOT NULL,
    "artist_name" varchar(2048) NOT NULL,
    "song_id" varchar(256) NOT NULL,
    "title" varchar(2048) NOT NULL,
    "duration" numeric(16, 8) NULL,
    "year" int NULL
)  
""")

songplay_table_create = ("""
CREATE TABLE "songplays" (
    "songplay_id" int IDENTITY(0,1) NOT NULL,
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
    "user_id" int UNIQUE NOT NULL,
    "first_name" varchar(2048) NULL,
    "last_name" varchar(2048) NULL,
    "gender" varchar(1) NULL,
    "level" varchar(16) NULL
) 
""")

song_table_create = ("""
CREATE TABLE "songs" (
    "song_id" varchar(256) UNIQUE NOT NULL,
    "artist_id" varchar(256) NOT NULL,
    "title" varchar(2048) NOT NULL,
    "duration" numeric(16, 8) NULL,
    "year" int NOT NULL    
) 
""")

artist_table_create = ("""
CREATE TABLE "artists" (
    "artist_id" varchar(256) UNIQUE NOT NULL,
    "name" varchar(2048) NOT NULL,
    "latitude" numeric(8,2) NULL,
    "longitude" numeric(8,2) NULL,
    "location" varchar(2048) NOT NULL
) 
""")

time_table_create = ("""
CREATE TABLE "times" (
    "start_time" timestamp UNIQUE NOT NULL,
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
    copy stg_events from {} 
    iam_role {}
    region 'us-west-2'
    FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy stg_songs from {} 
    iam_role {}
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (select  
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
        userId as user_id,
        level,
        s.song_id,
        s.artist_id,
        sessionId as session_id,
        location,
        userAgent as user_agent
     from stg_events ev
     join stg_songs s on ev.song=s.title
    );     
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    (select distinct 
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
     from stg_events
     where userId is not null);
""")

song_table_insert = ("""
    insert into songs (song_id, artist_id, title, duration, year)
    (select distinct 
        song_id,
        artist_id,
        title,
        duration,
        year
     from stg_songs);
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, longitude, latitude, location)
    (select distinct 
        artist_id,
        artist_name as name,
        artist_longitude as longitude,
        artist_latitude as latitude,
        artist_location as location
     from stg_songs);
""")

time_table_insert = ("""
    insert into times (start_time, hour, day, week, month, year, weekday)
    (select distinct
        start_time,
        extract(hour from start_time) as hour,
        extract(day from start_time) as day,
        extract(week from start_time) as week,
        extract(month from start_time) as month,
        extract(year from start_time) as year,
        extract(weekday from start_time) as weekday
    from songplays)
""")

# QUERY LISTS

# original
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



