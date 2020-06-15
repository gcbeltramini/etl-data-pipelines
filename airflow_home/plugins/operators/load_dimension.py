from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 table: str,
                 query: str,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Deleting all rows from Redshift table "{self.table}"')
        redshift.run(f'TRUNCATE {self.table:s};\n')

        self.log.info('Loading data into Redshift dimension table '
                      f'"{self.table}"')
        formatted_sql = (f'INSERT INTO {self.table:s}\n'
                         f'{self.query:s}')
        redshift.run(formatted_sql)
