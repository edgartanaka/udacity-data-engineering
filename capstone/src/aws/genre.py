import configparser
import psycopg2

CONFIG_FILE = 'aws.cfg'
SCHEMA = 'movielens'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)


def get_redshift_conn():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    return conn, cur


def insert_movielens_genre(conn, cur):
    # flatten values: tconst, genre
    # lowercase genre
    insert_flat_genres = """
    insert into analytics.genre (tconst, genre)

    """

    cur.execute(insert_flat_genres)
    conn.commit()


def insert_tmdb_genre(conn, cur):
    # select
    #   (row_number() over (order by true))::int as n
    # into tmdb.numbers
    # from tmdb.movies
    # limit 10;


def insert_imdb_genre(conn, cur):
    cur.execute("DROP TABLE IF EXISTS imdb.numbers")
    conn.commit()

    # trick to flatten comma separate value
    # assumes max of 10 commas
    # https://stackoverflow.com/questions/25112389/redshift-convert-comma-delimited-values-into-rows
    create_temp_numbers = """
    select 
      (row_number() over (order by true))::int as n
    into imdb.numbers
    from imdb.title_basics
    limit 10;
    """

    cur.execute(create_temp_numbers)
    conn.commit()

    cur.execute("DROP TABLE IF EXISTS movielens.numbers")
    conn.commit()

    # trick to flatten comma separate value
    # assumes max of 10 commas
    # https://stackoverflow.com/questions/25112389/redshift-convert-comma-delimited-values-into-rows
    create_temp_numbers = """
    select 
      (row_number() over (order by true))::int as n
    into movielens.numbers
    from movielens.movies
    limit 10;
    """

    cur.execute(create_temp_numbers)
    conn.commit()

    # flatten values: tconst, genre
    # lowercase genre
    insert_flat_genres = """
    insert into analytics.genre (tconst, genre)
    (
    -- all relationships movie-genre in IMDB
    WITH imdb_genres AS (
    select
      tconst,
      split_part(lower(genres),',',n) as genre
    from
      imdb.title_basics
    cross join
      numbers
    where
      split_part(genres,',',n) is not null
      and split_part(genres,',',n) != ''),
    
    -- all relationships movie-genre in movielens
    movielens_genres AS (
    select
    concat('tt', LPAD(l.imdbId, 7, 0)) as tconst,
	split_part(lower(m.genres),'|',n) as genre
    from movielens.movies m
    join movielens.links l on l.movieId=m.movieId
    cross join movielens.numbers
    where
	split_part(m.genres,'|',n) is not null
	and split_part(m.genres,'|',n) != ''
	),
	
	-- union all of the above
	all_genres AS (
	  select * from imdb_genres
	  union all
	  select * from movielens_genres
	)
	
	-- insert only distinct (without duplicates)
	select distinct tconst, genre from all_genres
    )
           
    """

    cur.execute(insert_flat_genres)
    conn.commit()


def main():
    """
    Main method.
    Loads events and songs data from S3 to Redshift (staging tables)
    Then, it extracts data from the Redshift staging tables, transforms and loads it
    into other Redshift tables to be queried by data analysts.
    :return: None
    """
    conn, curr = get_redshift_conn()
    insert_imdb_genre(conn, curr)
    # insert_tmdb_genre(conn, curr)
    # insert_movielens_genre(conn, curr)
    # remove_duplicates(conn, curr)
    conn.close()


if __name__ == "__main__":
    main()
