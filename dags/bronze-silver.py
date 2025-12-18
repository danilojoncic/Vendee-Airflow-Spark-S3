from airflow.sdk import DAG, task
from datetime import datetime, timedelta


START_DATE = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
STAGING_BUCKET = "staging"
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
AWS_CONN_ID = "s3_load_balanced"
SPARK_CONN_ID = "spark"
KEY = "waterline.txt"


with DAG(
    dag_id="spark-filter-parquet",
    start_date=START_DATE,
    schedule="0 * * * *",  # every hour,
    max_active_runs = 1,
    catchup=False,
    tags=["silver","spark", "s3"],

) as dag:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


    @task
    def ensure_waterline():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        bucket = "misc"

        if not hook.check_for_key(KEY, bucket_name=bucket):
            hook.load_string("00000000_000000", key=KEY, bucket_name=bucket, replace=True)

    @task
    def read_waterline():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        try:
            value = hook.read_key(KEY, bucket_name="misc")
            return value.strip()
        except Exception:
            return "00000000_000000"


    check = ensure_waterline()
    waterline = read_waterline()
    spark_task = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="s3a://jars/bronze_to_silver.jar",
        application_args=["--waterline", "{{ ti.xcom_pull(task_ids='read_waterline') }}"],
        conn_id=SPARK_CONN_ID,
        conf={
            # S3 credentials - these must be set as spark.hadoop properties
            'spark.hadoop.fs.s3a.access.key': "minio",
            'spark.hadoop.fs.s3a.secret.key': "minio123",
            'spark.hadoop.fs.s3a.endpoint': 'http://nginx:9000',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        },
    )

    check >> waterline >> spark_task


