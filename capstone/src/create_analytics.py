import configparser
import psycopg2

CONFIG_FILE = 'aws.cfg'
SCHEMA = 'analytics'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

def drop_schema(conn, cur):
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.commit()

def create_schema(conn, cur):
    cur.execute(f"CREATE SCHEMA {SCHEMA}")
    conn.commit()

def create_genre(conn, cur):
    create_table = """
    CREATE TABLE genre (
    tconst varchar(1024) NOT NULL,
    genre varchar(100) NOT NULL,
    UNIQUE(tconst, genre)
    )"""
    cur.execute(create_table)
    conn.commit()

def create_movie(conn, cur):
    create_table = """
    CREATE TABLE "movie" (
    "tconst" varchar(1024) UNIQUE NOT NULL PRIMARY KEY,
    "original_title" varchar(1024) NOT NULL,
    "overview" varchar(1024) NOT NULL,
    "release_date" varchar(1024) NOT NULL,
    "start_year" varchar(1024) NOT NULL,
    "end_year" varchar(1024) NOT NULL,
    "imdb_rating" varchar(1024) NOT NULL,
    "tmdb_rating" varchar(1024) NOT NULL,
    "movielens_rating" varchar(1024) NOT NULL,
    "original_language" varchar(1024) NOT NULL,
    "budget" bigint NULL,
    "revenue" bigint NULL,
    "status" varchar(1024) NOT NULL
    )"""
    cur.execute(create_table)
    conn.commit()

def create_person(conn, cur):
    pass

def create_award(conn, cur):
    pass

def create_tag(conn, cur):
    pass


def get_redshift_conn():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    return conn, cur

def main():
    """
    Main method.
    Loads events and songs data from S3 to Redshift (staging tables)
    Then, it extracts data from the Redshift staging tables, transforms and loads it
    into other Redshift tables to be queried by data analysts.
    :return: None
    """
    conn, curr = get_redshift_conn()

    # reset schema
    drop_schema(conn, curr)
    create_schema(conn, curr)

    # set all following queries to be applied on SCHEMA
    curr.execute(f"SET search_path TO {SCHEMA}")

    # create tables
    create_genre(conn, curr)
    create_movie(conn, curr)
    create_person(conn, curr)
    create_tag(conn, curr)

    conn.close()

if __name__ == "__main__":
    main()
