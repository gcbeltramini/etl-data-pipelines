from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 table: str,
                 query: str,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        self.log.info(f'Loading data into Redshift fact table "{self.table}"')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = (f'INSERT INTO {self.table:s}\n'
                         f'{self.query:s}')
        redshift.run(formatted_sql)
