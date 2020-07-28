from airflow.contrib.operators.bigquery_operator import BigQueryCreateExternalTableOperator
from airflow.utils.decorators import apply_defaults


class StageImdbOperator(BigQueryCreateExternalTableOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.field_delimiter = '\t',
        self.compression = 'GZIP',
        self.source_format = 'CSV',
        self.skip_leading_rows = 1
