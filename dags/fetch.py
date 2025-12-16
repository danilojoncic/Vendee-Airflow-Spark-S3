from airflow.sdk import DAG, task
from datetime import datetime

START_DATE = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
conn_id = "s3_load_balanced"



with DAG(
    dag_id="website-downloader",
    start_date=START_DATE,
    schedule="*/2 * * * *",
    catchup=False,
) as dag:

    @task
    def setup_urls():
        pass

    @task
    def ping_website():
        pass

    @task
    def ping_s3():
        pass

    @task
    def setup_s3_if_needed():
        pass

    @task
    def download_file():
        pass

    setup_urls() >> ping_website() >> ping_s3() >> setup_s3_if_needed() >> download_file()
dag