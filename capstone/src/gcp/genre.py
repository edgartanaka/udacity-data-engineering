"""
Creates analytics.movie Bigquery table
"""

from google.cloud import bigquery

client = bigquery.Client()
table_id = "edgart-experiments.analytics.genre"

job_config = bigquery.QueryJobConfig(destination=table_id)

sql = """

"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))