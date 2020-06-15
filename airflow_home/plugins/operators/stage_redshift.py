from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = ("COPY {table_name:s}\n"
                "FROM '{data_source:s}'\n"
                "ACCESS_KEY_ID '{aws_access_key_id:s}'\n"
                "SECRET_ACCESS_KEY '{aws_secret_access_key:s}'\n"
                "REGION '{region:s}'\n"
                "FORMAT JSON AS '{json_path:s}'\n"
                "EMPTYASNULL\n"
                "BLANKSASNULL;")

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 aws_credentials_id: str,
                 s3_bucket: str,
                 s3_key: str,
                 table: str,
                 aws_region: str = 'us-west-2',
                 json_path: str = 'auto',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.aws_region = aws_region
        self.json_path = json_path

    def execute(self, context):
        s3_key_formatted = self.s3_key.format(**context)
        s3_path = 's3://{bucket:s}/{key:s}'.format(bucket=self.s3_bucket,
                                                   key=s3_key_formatted)

        self.log.info(f'Copying data from "{s3_path:s}" to Redshift table '
                      f'"{self.table:s}"')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table_name=self.table,
            data_source=s3_path,
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region=self.aws_region,
            json_path=self.json_path,
        )
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(formatted_sql)
