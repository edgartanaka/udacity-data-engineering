import configparser
import psycopg2

CONFIG_FILE = 'aws.cfg'
SCHEMA = 'tmdb'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)



# connect to redshift
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

# create and set schema
cur.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA)
cur.execute("SET search_path TO " + SCHEMA)
conn.commit()

# drop table
query = f"DROP TABLE IF EXISTS movies"
cur.execute(query)
conn.commit()

# create table
movies_create = """
CREATE TABLE "movies" (
    "adult" boolean NULL,
    "backdrop_path" varchar(2000) NULL,
    "belongs_to_collection" varchar(2000) NULL,
    "budget" bigint NULL,
    "genres" varchar(4000) NULL,
    "homepage" varchar(2000) NULL,
    "id" int NULL,
    "imdb_id" varchar(32) NULL,
    "original_language" varchar(2) NULL,
    "original_title" varchar(4000) NULL,
    "overview" varchar(8000) NULL,
    "popularity" numeric(10,2) NULL,
    "poster_path" varchar(4000) NULL,
    "production_companies" varchar(4000) NULL,
    "production_countries" varchar(4000) NULL,
    "release_date" varchar(10) NULL,
    "revenue" bigint NULL,
    "runtime" bigint NULL,
    "spoken_languages" varchar(4000) NULL,
    "status" varchar(100) NULL,
    "tagline" varchar(4000) NULL,
    "title" varchar(4000) NULL,
    "video" boolean NULL,
    "vote_average" numeric(3,1) NULL,
    "vote_count" int NULL)
"""
cur.execute(movies_create)
conn.commit()

# stage
print('staging...')
movies_stage = ("""
    copy tmdb.movies FROM  {} 
    iam_role {}
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    json 'auto';
""").format(config['S3']['TMDB_BUCKET'], config['IAM_ROLE']['ARN'])
cur.execute(movies_stage)
conn.commit()
print('staging done!')

# close connection to redshift
conn.close()
