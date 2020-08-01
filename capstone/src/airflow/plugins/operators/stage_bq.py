from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict, BadRequest
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

class StageBigqueryOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 storage_uri,
                 table_id,
                 schema,
                 field_delimiter=',',
                 null_marker='',
                 quote_character='"',
                 *args, **kwargs):
        super(StageBigqueryOperator, self).__init__(*args, **kwargs)
        self.storage_uri = storage_uri
        self.table_id = table_id
        self.schema = schema
        self.field_delimiter = field_delimiter
        self.null_marker = null_marker
        self.quote_character = quote_character

    def execute(self, context):
        key_path = Variable.get('service_account_path')
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id, )

        print("************** Started STAGE BQ!!! *******************")

        job_config = bigquery.LoadJobConfig(
            schema=self.schema,
            skip_leading_rows=1,
            field_delimiter=self.field_delimiter,
            source_format=bigquery.SourceFormat.CSV,
            quote_character=self.quote_character,
            null_marker=self.null_marker,
            write_disposition='WRITE_TRUNCATE',
        )

        load_job = client.load_table_from_uri(
            self.storage_uri, self.table_id, job_config=job_config
        )

        try:
            load_job.result()
        except Exception:
            for e in load_job.errors:
                print('ERROR: {}'.format(e['message']))

        table = client.get_table(self.table_id)
        print("Loaded {} rows to table {}".format(table.num_rows, self.table_id))