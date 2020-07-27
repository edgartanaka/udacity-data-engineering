import configparser
import psycopg2

CONFIG_FILE = 'aws.cfg'
SCHEMA = 'movielens'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

tables = ['genome_scores', 'genome_tags', 'links', 'movies', 'ratings']

# CREATE TABLES
genome_scores_create = ("""
CREATE TABLE "genome_scores" (
    "movieId" int NOT NULL,
    "tagId" int NOT NULL,
    "relevance" numeric(24,1) NOT NULL
)  
""")

genome_tags_create = ("""
CREATE TABLE "genome_tags" (
    "tagId" int NOT NULL,
    "tag" varchar(2048) NOT NULL
)  
""")

movies_create = ("""
CREATE TABLE "movies" (
    "movieId" int NOT NULL,
    "title" varchar(2048) NOT NULL,
    "genres" varchar(2048) NOT NULL
)  
""")

links_create = ("""
CREATE TABLE "links" (
    "movieId" int NOT NULL,
    "imdbId" int NOT NULL,
    "tmdbId" int NULL
)
""")

ratings_create = ("""
CREATE TABLE "ratings" (
    "userId" int NOT NULL,
    "movieId" int NOT NULL,
    "rating" numeric(3,1) NOT NULL,
    "timestamp" int NOT NULL 
)  
""")


# STAGING TABLES

class StageS3ToRedshift(object):
    def __init__(self, table_name, s3_path, region, arn):
        self.table_name = table_name
        self.s3_path = s3_path
        self.arn = arn
        self.region = region

    def query(self):
        query = ("""
            copy {} from {} 
            iam_role {}
            region {}
            ignoreheader 1
            delimiter '\t' gzip;
        """).format(self.table_name,
                    self.s3_path,
                    self.arn,
                    self.region)

        return query


def load_staging_tables(cur, conn):
    """
    Loads events and songs data from S3 to Redshift (staging tables)
    :param cur: psycopg2 cursor
    :param conn: psycopg2 connection
    :return: None
    """
    for t in tables:
        s3_path = '\'{}{}.csv\''.format(config['S3']['MOVIELENS_BUCKET'], t.replace('_', '-'))
        query = ("""
            copy {} from {} 
            iam_role {}
            region {}            
            ignoreheader 1
            csv;
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
    create_table_queries = [movies_create,
                            genome_tags_create,
                            genome_scores_create,
                            ratings_create,
                            links_create]
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def get_redshift_conn():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    cur.execute("SET search_path TO " + SCHEMA)
    return conn, cur

def create_schema():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
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
