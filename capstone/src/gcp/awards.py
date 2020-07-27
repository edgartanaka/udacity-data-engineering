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


def load_oscars():
    job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition='WRITE_APPEND', )

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
      m.award_year,
      m.award_category,
      m.award_winner,
      m.film,
      m.person_name,
      MAX(m.tconst) as tconst, -- keeps non null value
      MAX(m.nconst) as nconst -- keeps non null value
      FROM all_oscars m
      GROUP BY 1,2,3,4,5
    """

    # Start the query, passing in the extra configuration.
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))


def load_golden_globe():
    pass


def load_bafta():
    pass


def drop_table():
    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))


def main():
    drop_table()
    load_bafta()
    load_oscars()
    load_golden_globe()


if __name__ == "__main__":
    main()
