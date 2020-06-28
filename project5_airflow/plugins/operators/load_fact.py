from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    facts_sql_template = """
        INSERT INTO {destination_table}
        ({load_sql})
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 load_sql="",
                 mode="append",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.load_sql = load_sql
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        facts_sql = LoadFactOperator.facts_sql_template.format(
            destination_table=self.destination_table,
            load_sql=self.load_sql
        )

        if self.mode == "delete":
            self.log.info(f"Clearing data from destination Redshift table {self.destination_table}")
            redshift.run("DELETE FROM {}".format(self.destination_table))

        self.log.info(f"Loading data into fact table {self.destination_table}")
        redshift.run(facts_sql)

        self.log.info(f"LOADED data into fact table {self.destination_table}")