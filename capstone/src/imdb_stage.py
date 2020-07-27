import configparser
import psycopg2

CONFIG_FILE = 'aws.cfg'
SCHEMA = 'imdb'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

# Redshift table names
tables = ['name_basics', 'title_basics', 'title_crew', 'title_principals', 'title_ratings']

# CREATE TABLES
imdb_name_basics_create = ("""
CREATE TABLE "name_basics" (
    "nconst" varchar(2048) NOT NULL,
    "primaryName" varchar(2048) NOT NULL,
    "birthYear" varchar(4) NULL,
    "deathYear" varchar(4) NULL,
    "primaryProfession" varchar(2048) NULL,
    "knownForTitles" varchar(2048) NULL
)  
""")

imdb_title_ratings_create = ("""
CREATE TABLE "title_ratings" (
    "nconst" varchar(16) NOT NULL,
    "averageRating" numeric(3,1) NOT NULL,
    "numVotes" int NOT NULL
)  
""")

imdb_title_principals_create = ("""
CREATE TABLE "title_principals" (
    "tconst" varchar(16) NOT NULL,
    "ordering" int NOT NULL,
    "nconst" varchar(16) NOT NULL,
    "category" varchar(2048) NULL,
    "job" varchar(2048) NULL,
    "characters" varchar(2048) NULL
)  
""")

imdb_title_crew_create = ("""
CREATE TABLE "title_crew" (
    "tconst" varchar(16) NOT NULL,
    "directors" varchar(16000) NULL,
    "writers" varchar(16000) NULL
)
""")

imdb_title_basics_create = ("""
CREATE TABLE "title_basics" (
    "tconst" varchar(16) NOT NULL,
    "titleType" varchar(32) NOT NULL,
    "primaryTitle" varchar(1024) NOT NULL,
    "originalTitle" varchar(1024) NOT NULL,
    "isAdult" int NOT NULL,
    "startYear" varchar(4) NULL,
    "endYear" varchar(4) NULL,
    "runtimeMinutes" int NULL,
    "genres" varchar(1024) NULL      
)  
""")


# STAGING TABLES

# class StageS3ToRedshift(object):
#     def __init__(self, table_name, s3_path, region, arn):
#         self.table_name = table_name
#         self.s3_path = s3_path
#         self.arn = arn
#         self.region = region
#
#     def query(self):
#         query = ("""
#             copy {} from {}
#             iam_role {}
#             region {}
#             ignoreheader 1
#             delimiter '\t' gzip;
#         """).format(self.table_name,
#                     self.s3_path,
#                     self.arn,
#                     self.region)
#
#         return query


def load_staging_tables(cur, conn):
    """
    Loads events and songs data from S3 to Redshift (staging tables)
    :param cur: psycopg2 cursor
    :param conn: psycopg2 connection
    :return: None
    """

    for t in tables:
        s3_path = '\'{}{}.tsv.gz\''.format(config['S3']['IMDB_BUCKET'], t.replace('_', '.'))
        query = ("""
            copy {} from {} 
            iam_role {}
            region {}
            ignoreheader 1
            delimiter '\t' gzip;
        """).format(t, s3_path, config['IAM_ROLE']['ARN'], config['CLUSTER']['REGION'])

        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """
    Drops all Redshift tables in this project.
    :param cur: psycopg2 cursor
    :param conn: psycopg2 connection
    :return:
    """
    for table_name in tables:
        query = f"DROP TABLE IF EXISTS {table_name}"
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all Redshift tables in this project.
    :param cur: psycopg2 cursor
    :param conn: psycopg2 connection
    :return:
    """
    create_table_queries = [imdb_name_basics_create,
                            imdb_title_basics_create,
                            imdb_title_ratings_create,
                            imdb_title_principals_create,
                            imdb_title_crew_create]
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def get_redshift_conn():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    cur.execute("SET search_path TO " + SCHEMA)
    return conn, cur


def create_schema():
    conn, cur = get_redshift_conn()
    cur.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA)
    conn.commit()

def drop_create_tables():
    """
    Main method.
    :return:
    """
    conn, cur = get_redshift_conn()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


def stage_tables():
    conn, cur = get_redshift_conn()
    load_staging_tables(cur, conn)
    conn.close()


def main():
    """
    Main method.
    Loads events and songs data from S3 to Redshift (staging tables)
    Then, it extracts data from the Redshift staging tables, transforms and loads it
    into other Redshift tables to be queried by data analysts.
    :return: None
    """
    create_schema()
    drop_create_tables()
    stage_tables()


if __name__ == "__main__":
    main()
