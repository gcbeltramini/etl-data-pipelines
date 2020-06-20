from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.constants import REDSHIFT_CONN_ID


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = REDSHIFT_CONN_ID,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def check_result_not_empty(self, records: list, table: str) -> bool:
        contains_result = True
        if len(records) < 1 or len(records[0]) < 1:
            contains_result = False
            self.log.error('Data quality check failed: '
                           f'"{table:s}" returned no results.')
        return contains_result

    def has_valid_count(self, redshift: PostgresHook) -> bool:
        comparisons = (
            {'table1': 'public.staging_events', 'col': 'userid',
             'table2': 'public.users'},
            {'table1': 'public.staging_events', 'col': 'ts',
             'table2': 'public.time'},
            {'table1': 'public.staging_songs', 'col': 'song_id',
             'table2': 'public.songs'},
            {'table1': 'public.staging_songs', 'col': 'artist_id',
             'table2': 'public.artists'},
        )

        has_error = False

        for comparison in comparisons:
            table1 = comparison['table1']
            col = comparison['col']
            table2 = comparison['table2']

            sql = f'SELECT COUNT(DISTINCT {col:s}) FROM {table1:s};'
            records1 = redshift.get_records(sql)
            self.log.info(f'{sql:s}\nResult = {records1}')

            sql = f'SELECT COUNT(*) FROM {table2:s};'
            records2 = redshift.get_records(sql)
            self.log.info(f'{sql:s}\nResult = {records2}')

            contains_result1 = self.check_result_not_empty(records1, table1)
            contains_result2 = self.check_result_not_empty(records2, table2)

            if not contains_result1 or not contains_result2:
                has_error = True
                continue

            num_records1 = records1[0][0]
            num_records2 = records2[0][0]

            self.log.info(f'There are {num_records1:d} distinct values in '
                          f'column "{col:s}" of table "{table1:s}"')
            self.log.info(f'There are {num_records2:d} rows in '
                          f'table "{table2:s}"')

            if num_records1 > num_records2:
                has_error = True
                self.log.error(
                    f'Expected {num_records1:d} <= {num_records2:d}')
            else:
                self.log.info(f'Success: {num_records1:d} <= {num_records2:d}')

        return has_error

    def has_valid_nulls(self, redshift: PostgresHook) -> bool:
        has_error = False
        not_null_cols = (
            {'table': 'public.artists', 'cols': ['artistid']},
            {'table': 'public.songplays', 'cols': ['playid', 'start_time']},
            {'table': 'public.songs', 'cols': ['songid']},
            {'table': 'public."time"', 'cols': ['start_time']},
            {'table': 'public.users', 'cols': ['userid']},
        )
        sql = 'SELECT COUNT(*) FROM {table:s} WHERE {col:s} IS NULL;'
        for t in not_null_cols:
            table = t['table']
            for col in t['cols']:
                records = redshift.get_records(sql.format(table=table,
                                                          col=col))
                contains_result = self.check_result_not_empty(records,
                                                              t['table'])

                if not contains_result:
                    has_error = True
                    continue

                num_records = records[0][0]

                self.log.info(f'There are {num_records:d} NULL values in '
                              f'column "{col:s}" of table "{table:s}"')

                if num_records > 0:
                    has_error = True
                    self.log.error(f'Expected {num_records:d} = 0')
                else:
                    self.log.info(f'Success: {num_records:d} = 0')

        return has_error

    def execute(self, context):
        self.log.info(f'Running data quality check')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        has_error = [self.has_valid_count(redshift),
                     self.has_valid_nulls(redshift)]

        if any(has_error):
            raise ValueError('Data quality check failed. '
                             'Check the error log messages.')
        else:
            self.log.info('Data quality check finished successfully.')
