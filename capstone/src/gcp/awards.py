"""
Creates analytics.movie Bigquery table
"""

from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "../../credentials/edgart-experiments-67ca4ddbda73.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
table_id = "{}.analytics.awards".format(credentials.project_id)
job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition='WRITE_APPEND', )


def load_oscars():
    sql = """
    WITH oscars_person AS (
      SELECT  
      o.year_ceremony as award_year,
      o.category as award_category,
      o.winner as award_winner,  
      o.film as film,
      o.name as person_name,  
      m.tconst as tconst, 
      p.nconst as nconst
      FROM `edgart-experiments.awards.oscars` o
      JOIN `edgart-experiments.analytics.person` p ON lower(p.primary_name)=lower(o.name)
      JOIN `edgart-experiments.analytics.movie` m ON lower(o.film)=lower(m.primary_title) AND o.year_filme=CAST(start_year AS INT64)
      JOIN `edgart-experiments.analytics.movie_person` mp ON mp.nconst=p.nconst AND m.tconst=mp.tconst
    ),
    oscars_movie AS (
      SELECT  
      o.year_ceremony as award_year,
      o.category as award_category,
      o.winner as award_winner,
      o.film as film,
      o.name as person_name,
      m.tconst as tconst, 
      cast(null as string) as nconst
      FROM `edgart-experiments.awards.oscars` o
      JOIN `edgart-experiments.analytics.movie` m ON lower(o.film)=lower(m.primary_title) AND o.year_filme=CAST(m.start_year AS INT64)
    ),
    all_oscars AS (
      SELECT * FROM oscars_movie
      UNION ALL
      SELECT * FROM oscars_person
      )
      
    -- removes duplicates while keeping non null values
    SELECT 
      'oscars' as award_name,
      m.award_year,
      m.award_category,
      m.award_winner,
      m.film,
      m.person_name,
      MAX(m.tconst) as tconst, -- keeps non null value
      MAX(m.nconst) as nconst -- keeps non null value
      FROM all_oscars m
      GROUP BY 1,2,3,4,5,6
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))


def load_golden_globe():
    sql = """
    WITH golden_globe_person AS (
      SELECT  
      gg.year_award as award_year,
      gg.category as award_category,
      gg.win as award_winner,  
      gg.film as film,
      gg.nominee as person_name,  
      m.tconst as tconst, 
      p.nconst as nconst
      FROM `edgart-experiments.awards.golden_globe` gg
      JOIN `edgart-experiments.analytics.person` p ON lower(p.primary_name)=lower(gg.nominee)
      JOIN `edgart-experiments.analytics.movie` m ON lower(gg.film)=lower(m.primary_title) AND gg.year_film=CAST(start_year AS INT64)
      JOIN `edgart-experiments.analytics.movie_person` mp ON mp.nconst=p.nconst AND m.tconst=mp.tconst
      WHERE gg.category NOT LIKE '%Television%' AND gg.category NOT LIKE '%Series%' -- remove TV series
    ),
    golden_globe_movie AS (
      SELECT  
      gg.year_award as award_year,
      gg.category as award_category,
      gg.win as award_winner,  
      gg.film as film,
      gg.nominee as person_name,  
      m.tconst as tconst, 
      p.nconst as nconst
      FROM `edgart-experiments.awards.golden_globe` gg
      JOIN `edgart-experiments.analytics.person` p ON lower(p.primary_name)=lower(gg.nominee)
      JOIN `edgart-experiments.analytics.movie` m ON lower(gg.film)=lower(m.primary_title) AND gg.year_film=CAST(start_year AS INT64)
      WHERE gg.category NOT LIKE '%Television%' AND gg.category NOT LIKE '%Series%' -- remove TV series
    ),
    all_golden_globe AS (
      SELECT * FROM golden_globe_movie
      UNION ALL
      SELECT * FROM golden_globe_person
      )
      
    -- removes duplicates while keeping non null values
    SELECT 
      'golden_globe' as award_name,
      m.award_year,
      m.award_category,
      m.award_winner,
      m.film,
      m.person_name,
      MAX(m.tconst) as tconst, -- keeps non null value
      MAX(m.nconst) as nconst -- keeps non null value
      FROM all_golden_globe m
      GROUP BY 1,2,3,4,5,6
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))


def load_saga():
    sql = """
    SELECT  
      'SAG Awards' as award_name,
      SUBSTR(saga.year, 0, 4) as award_year,
      saga.category as award_category,
      saga.won as award_winner,  
      saga.show as film,
      saga.full_name as person_name,  
      m.tconst as tconst, 
      p.nconst as nconst
    FROM `edgart-experiments.awards.saga` saga
    JOIN `edgart-experiments.analytics.person` p ON LOWER(p.primary_name)=LOWER(saga.full_name)
    JOIN `edgart-experiments.analytics.movie` m ON LOWER(saga.show)=LOWER(m.primary_title)
    JOIN `edgart-experiments.analytics.movie_person` mp ON mp.nconst=p.nconst AND m.tconst=mp.tconst
    WHERE 
      LOWER(saga.category) NOT LIKE '%television%' AND LOWER(saga.category) NOT LIKE '%series%' -- remove TV series
      AND saga.year IS NOT NULL
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))


def load_bafta():
    sql = """
        WITH movies_matching_name_year AS (
        -- nominee is a movie name
          SELECT  
            bafta.nominee as movie_name,
            COALESCE(m1.tconst, m2.tconst) as tconst
        FROM `edgart-experiments.awards.bafta` bafta
        LEFT JOIN `edgart-experiments.analytics.movie` m1 ON LOWER(m1.primary_title)=LOWER(bafta.nominee) AND CAST(m1.start_year AS INT64)=bafta.year
        LEFT JOIN `edgart-experiments.analytics.movie` m2 ON LOWER(m2.primary_title)=LOWER(bafta.nominee) AND CAST(m2.start_year AS INT64)=(bafta.year-1)
        
        UNION ALL
        
        -- workers is a movie name
          SELECT  
            bafta.workers as movie_name,
            COALESCE(m1.tconst, m2.tconst) as tconst
        FROM `edgart-experiments.awards.bafta` bafta
        LEFT JOIN `edgart-experiments.analytics.movie` m1 ON LOWER(m1.primary_title)=LOWER(bafta.nominee) AND CAST(m1.start_year AS INT64)=bafta.year
        LEFT JOIN `edgart-experiments.analytics.movie` m2 ON LOWER(m2.primary_title)=LOWER(bafta.workers) AND CAST(m2.start_year AS INT64)=(bafta.year-1)
        ),
        clean_movies AS (
          SELECT DISTINCT movie_name, tconst
          FROM movies_matching_name_year
          WHERE tconst IS NOT NULL
        ),
        nominee_is_movie AS (
        -- nominee is a movie name
        SELECT  
          bafta.year as award_year,
          bafta.category as award_category,
          bafta.winner as award_winner,  
          bafta.nominee as film,
          CAST(NULL AS STRING) as person_name,  -- this is a movie award, no person associated
          m.tconst as tconst, 
          CAST(NULL AS STRING) as nconst  -- this is a movie award, no person associated
        FROM `edgart-experiments.awards.bafta` bafta
        JOIN `edgart-experiments.analytics.person` p ON LOWER(p.primary_name)=LOWER(SPLIT(bafta.workers)[OFFSET(0)])
        JOIN clean_movies m ON LOWER(m.movie_name)=LOWER(bafta.nominee)
        ),
        nominee_is_person AS (
        -- catch cases where nominee is the person name and workers is the movie name
        SELECT  
          bafta.year as award_year,
          bafta.category as award_category,
          bafta.winner as award_winner,  
          bafta.workers as film,
          bafta.nominee as person_name,  
          m.tconst as tconst, 
          p.nconst as nconst
        FROM `edgart-experiments.awards.bafta` bafta
        JOIN `edgart-experiments.analytics.person` p ON LOWER(p.primary_name)=LOWER(bafta.nominee)
        JOIN clean_movies m ON LOWER(m.movie_name)=LOWER(bafta.nominee)
        JOIN `edgart-experiments.analytics.movie_person` mp ON mp.nconst=p.nconst AND m.tconst=mp.tconst
        ),
        only_nominee_no_worker AS (
        -- catch cases where nominee is a movie name and workers is null
        -- join movie name + year just to be safe - we try joining movie.start_date with the year of the award and the previous year
        -- this should catch mostly old cases from the 50s decade
        SELECT  
          bafta.year as award_year,
          bafta.category as award_category,
          bafta.winner as award_winner,  
          bafta.nominee as film,
          bafta.workers as person_name,  
          m.tconst, 
          CAST(NULL AS STRING) as nconst
        FROM `edgart-experiments.awards.bafta` bafta
        JOIN clean_movies m ON LOWER(m.movie_name)=LOWER(bafta.nominee)
        WHERE bafta.workers IS NULL
        ),
        all_bafta AS (
          SELECT DISTINCT * FROM (
            SELECT * FROM nominee_is_movie
            UNION ALL
            SELECT * FROM nominee_is_person
            UNION ALL
            SELECT * FROM only_nominee_no_worker
          )
        )
        select * from all_bafta 
    """


def drop_table():
    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))


def main():
    drop_table()
    load_saga()
    load_oscars()
    load_golden_globe()
    # load_bafta() # there are lots of data issues with the raw data


if __name__ == "__main__":
    main()
