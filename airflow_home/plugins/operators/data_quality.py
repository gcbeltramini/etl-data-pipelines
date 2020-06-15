from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def check_result_not_empty(self, records: list, table: str) -> bool:
        contains_result = False
        if len(records) < 1 or len(records[0]) < 1:
            contains_result = True
            self.log.error('Data quality check failed: '
                           f'"{table:s}" returned no results.')
        return contains_result

    def execute(self, context):
        self.log.info(f'Running data quality check')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        comparisions = (
            {'table1': 'staging_events', 'col': 'userid',
             'table2': 'users'},
            {'table1': 'staging_songs', 'col': 'song_id',
             'table2': 'songs'},
            {'table1': 'staging_songs', 'col': 'artist_id',
             'table2': 'artists'},
            {'table1': 'staging_events', 'col': 'ts',
             'table2': 'time'},
        )

        has_error = False

        for comparision in comparisions:
            table1 = comparision['table1']
            col = comparision['col']
            table2 = comparision['table2']

            sql = f'SELECT COUNT(DISTINCT {col:s}) FROM {table1:s};'
            records1 = redshift.get_records(sql)

            sql = f'SELECT COUNT(*) FROM {table2:s};'
            records2 = redshift.get_records(sql)

            contains_result1 = self.check_result_not_empty(records1, table1)
            contains_result2 = self.check_result_not_empty(records2, table2)

            if not contains_result1 and not contains_result2:
                has_error = True
                continue

            num_records1 = records1[0][0]
            num_records2 = records2[0][0]

            self.log.info(f'There are {num_records1:d} distinct values in '
                          f'column "{col:s}" of table "{table1:s}"')
            self.log.info(f'There are {num_records2:d} rows in '
                          f'table "{table2:s}"')

            if num_records1 != num_records2:
                has_error = True
                self.log.error(f'Expected {num_records1:d} = {num_records2:d}')

        if has_error:
            raise ValueError('Data quality check failed. '
                             'Check the error log messages.')
        else:
            self.log.info('Data quality check finished successfully.')
