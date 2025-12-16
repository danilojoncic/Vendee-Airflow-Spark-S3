from airflow.sdk import DAG, task
from datetime import datetime

# ---------------- CONFIG ----------------
START_DATE = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
CONN_ID = "s3_load_balanced"
BUCKETS = ["staging", "jars", "scripts", "misc", "bronze", "silver", "gold"]
BUCKET_FOR_URLS = "misc"
URL_KEY = "urls.txt"
MAIN_URL_TEMPLATE = "https://www.vendeeglobe.org/sites/default/files/ranking/vendeeglobe_leaderboard_{}.xlsx"
START_TS = "20241110_100000"
END_TS = "20250308_070000"
STEP_HOURS = 4

def generate_timestamps(start_date: str, end_date: str, step_hours: int = STEP_HOURS):
    from datetime import datetime, timedelta
    start = datetime.strptime(start_date, "%Y%m%d_%H%M%S")
    end = datetime.strptime(end_date, "%Y%m%d_%H%M%S")
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d_%H%M%S")
        current += timedelta(hours=step_hours)

with (DAG(
    dag_id="website_downloader_s3_fifo",
    start_date=START_DATE,
    schedule="*/2 * * * *",
    catchup=False,
    tags=["s3", "downloader"]
) as dag):

    @task
    def ensure_buckets_exist():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=CONN_ID)
        s3_client = hook.get_conn()
        existing_buckets = [b['Name'] for b in s3_client.list_buckets().get('Buckets', [])]
        for bucket in BUCKETS:
            if bucket not in existing_buckets:
                hook.create_bucket(bucket)
                print(f"Created bucket: {bucket}")
        return BUCKET_FOR_URLS  # return bucket to use for URLs

    @task
    def setup_urls(bucket_name: str):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=CONN_ID)
        if not hook.check_for_key(URL_KEY, bucket_name):
            urls = [MAIN_URL_TEMPLATE.format(ts) for ts in generate_timestamps(START_TS, END_TS)]
            hook.load_string("\n".join(urls), URL_KEY, bucket_name=bucket_name, replace=True)
            print(f"Initialized URL list in {bucket_name}/{URL_KEY}")
        return bucket_name

    @task
    def get_next_url(bucket_name: str):
        from airflow.exceptions import AirflowSkipException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=CONN_ID)
        if not hook.check_for_key(URL_KEY, bucket_name):
            raise AirflowSkipException("No URLs left to process.")

        content = hook.read_key(URL_KEY, bucket_name=bucket_name)
        urls = content.splitlines()
        if not urls:
            raise AirflowSkipException("URL list empty, nothing to download.")

        next_url = urls.pop(0)
        hook.load_string("\n".join(urls), URL_KEY, bucket_name=bucket_name, replace=True)
        return next_url

    @task
    def ping_website():
        import requests

        response = requests.head("https://www.vendeeglobe.org", timeout=10)
        if response.status_code != 200:
            raise Exception(f"Website not reachable")
        return True

    @task
    def download_file(url: str, bucket_name: str):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import requests

        hook = S3Hook(aws_conn_id=CONN_ID)
        key = url.split("/")[-1]

        if hook.check_for_key(key, bucket_name):
            return f"Skipped {key}, already exists"

        r = requests.get(url)
        if r.status_code == 200:
            hook.load_bytes(r.content, key, bucket_name=bucket_name, replace=False)
            return f"Uploaded {key}"
        else:
            raise Exception(f"Failed to download {url}")

    # ---------------- TASK FLOW ----------------
    bucket_name = ensure_buckets_exist()
    setup_urls(bucket_name)
    next_url = get_next_url(bucket_name)
    ping_website() >> download_file(next_url, bucket_name)
