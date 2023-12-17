from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class S3ToRedshift(BaseOperator):
    """
    Copy data from S3 to Redshift using Airflow connections

    ### Args:
    conn_id: name of Redshift's Airflow connection
    aws_credentials: name of AWS credentials in the Airflow connection
    table: table name
    s3_bucket: S3 bucket name
    s3_key: S3 path inside the bucket

    ### Example:
    ```
    stage_table_to_redshift = StageToRedshiftOperator(
        task_id='stage_table',
        dag=dag,
        conn_id="redshift",
        aws_credentials="aws_credentials",
        table="public.staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
    )
    ```
    """

    def __init__(
        self,
        conn_id="",
        aws_credentials="",
        table="",
        s3_bucket="",
        s3_key="",
        *args,
        **kwargs,
    ):
        super(S3ToRedshift, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context: dict):
        aws_hook = AwsBaseHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()  # get credentials from Airflow

        rds_hook = PostgresHook(postgres_conn_id=self.conn_id)

        # Use Airflow context var to replace year and month
        self.s3_key = self.s3_key.format(
            year=context["data_interval_start"].year,
            month="%02d" % context["data_interval_start"].month,
        )

        # s3 path
        path = f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info(f"Copying file {path} to Redshift {self.table} table")

        sql = f"""
            COPY {self.table}
            FROM '{path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            IGNOREHEADER 1
            CSV;
        """
        rds_hook.run(sql)
