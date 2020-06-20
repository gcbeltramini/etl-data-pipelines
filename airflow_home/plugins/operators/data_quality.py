from typing import Any, Dict, Sequence

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.constants import REDSHIFT_CONN_ID


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 count_comparisons: Sequence[Dict[str, Any]],
                 not_null_cols: Sequence[Dict[str, Any]],
                 redshift_conn_id: str = REDSHIFT_CONN_ID,
                 *args, **kwargs):
        """
        Data quality operator. Run all checks, logging any error. If there are
        errors, all of them are logged and an error is raised only after all
        checks ran.

        Parameters
        ----------
        count_comparisons : sequence[dict]
            Compare the number of distinct rows in column "col" of "table1",
            and the number of rows in "table2". The first number must be less
            than or equal to the second number. For example,

            comparisons = ({'table1': 'my_table', 'col': 'my_col',
                            'table2': 'another_table'},)

            will check if the number of distinct rows in `my_table.my_col` is
            less than or equal to the number of rows in `another_table`.

            There can be an arbitrary number of comparisons; the only
            restriction is that the keys in the `dict` must be "table1", "col"
            and "table2".
        not_null_cols : sequence[dict]
            Check if there is any NULL value in the given columns. For example,

            not_null_cols = ({'table': 'my_table', 'cols': ['c1', 'c2']},)

            will check if there is any NULL values in columns `my_table.c1` and
            `my_table.c2`.

            There can be an arbitrary number of checks; the only restriction is
            that the keys in the `dict` must be "table" and "cols".
        redshift_conn_id : str, optional
        *args, **kwargs
            Additional arguments to pass to `airflow.models.BaseOperator`
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.count_comparisons = count_comparisons
        self.not_null_cols = not_null_cols

    def check_result_not_empty(self, records: list, table: str) -> bool:
        contains_result = True
        if len(records) < 1 or len(records[0]) < 1:
            contains_result = False
            self.log.error('Data quality check failed: '
                           f'"{table:s}" returned no results.')
        return contains_result

    def has_valid_count(self, redshift: PostgresHook) -> bool:
        has_error = False

        for comparison in self.count_comparisons:
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
        sql = 'SELECT COUNT(*) FROM {table:s} WHERE {col:s} IS NULL;'
        for t in self.not_null_cols:
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
