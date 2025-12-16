from airflow.sdk import DAG, task
from datetime import datetime
from airflow.datasets import Dataset

staging_events = Dataset("s3://staging")

START_DATE = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
CONN_ID = "s3_load_balanced"
BUCKETS = ["staging", "jars", "scripts", "misc", "bronze", "silver", "gold"]
BUCKET_FOR_URLS = "misc"



with (DAG(
    dag_id="converter",
    start_date=START_DATE,
    schedule=[staging_events],
    catchup=False,
    tags=["s3", "convert"]
) as dag):
    @task
    def do_after_event():
        print("Thats me being called after an event")


    do_after_event()



