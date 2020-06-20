from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.constants import REDSHIFT_CONN_ID


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table: str,
                 query: str,
                 redshift_conn_id: str = REDSHIFT_CONN_ID,
                 delete: bool = True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.delete = delete

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete:
            self.log.info('Deleting all rows from Redshift table '
                          f'"{self.table}"')
            redshift.run(f'TRUNCATE {self.table:s};')
        else:
            self.log.info(f'Appending rows into Redshift table "{self.table}"')

        self.log.info('Loading data into Redshift dimension table '
                      f'"{self.table}"')
        formatted_sql = (f'INSERT INTO {self.table:s}\n'
                         f'{self.query:s}')
        redshift.run(formatted_sql)
