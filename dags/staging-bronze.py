from airflow.sdk import DAG, task
from datetime import datetime, timedelta
from airflow.sdk.definitions.asset import Dataset

staging_events = Dataset("s3://staging")
START_DATE = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
STAGING_BUCKET = "staging"
BRONZE_BUCKET = "bronze"
AWS_CONN_ID = "s3_load_balanced"


with DAG(
    dag_id="xlsx_to_csv_bronze_dynamic",
    start_date=START_DATE,
    schedule=[staging_events],
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "s3"],
) as dag:
    @task
    def list_xlsx_files():
        from airflow.exceptions import AirflowException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        keys = hook.list_keys(bucket_name=STAGING_BUCKET)

        if not keys:
            raise AirflowException(f"No keys found for {STAGING_BUCKET}")

        xlsx_files = [k for k in keys if k.endswith(".xlsx")]
        return xlsx_files


    @task()
    def convert_single_file(key: str):
        from io import BytesIO
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import pandas as pd

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        obj = hook.get_key(key, bucket_name=STAGING_BUCKET)
        data = obj.get()["Body"].read()

        df_check = pd.read_excel(
            BytesIO(data),
            engine="calamine",
            skiprows=3,
            nrows=1,
        )

        if any(
                "arrivée" in str(col).lower()
                or "arrival date" in str(col).lower()
                for col in df_check.columns
        ):
            df_full = pd.read_excel(BytesIO(data), engine="calamine", header=None)
            beginning_of_racing_rows = 3

            for idx in range(len(df_full)):
                row_values = df_full.iloc[idx].astype(str).str.cat(sep=" ")
                if "Depuis 30 minutes" in row_values or "Since 30 minutes" in row_values:
                    beginning_of_racing_rows = idx
                    break
        else:
            beginning_of_racing_rows = 3

        df = pd.read_excel(
            BytesIO(data),
            engine="calamine",
            skiprows=beginning_of_racing_rows,
        )

        df = df.iloc[:, 1:]
        df = df.iloc[:-4, :]
        df = df.drop(0, errors="ignore")

        df.columns = (
            df.columns.str.replace(r"[\r\n]+", " ", regex=True)
            .str.strip()
        )

        df = df.map(
            lambda x: str(x)
            .replace("\r\n", " ")
            .replace("\n", " ")
            .replace("\r", " ")
            .strip()
            if isinstance(x, str)
            else x
        )

        first_col = df.columns[0]
        df = df[
            ~df[first_col].astype(str).str.strip().isin(["RET", "DNF", "ARV"])
        ]

        df = df.reset_index(drop=True)

        new_columns = [
            "rank",
            "nationality_sail",
            "skipper_boat",
            "time",
            "latitude",
            "longitude",
            "heading_30min",
            "speed_30min",
            "avg_speed_30min",
            "distance_30min",
            "heading_last_report",
            "speed_last_report",
            "avg_speed_last_report",
            "distance_last_report",
            "heading_24h",
            "speed_24h",
            "avg_speed_24h",
            "distance_24h",
            "dtf",
            "dtl",
        ]

        if len(df.columns) != len(new_columns):
            new_columns = new_columns[: len(df.columns)]

        df.columns = new_columns

        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        csv_key = key.replace(".xlsx", ".csv")

        hook.load_bytes(
            bytes_data=csv_buffer.getvalue(),
            key=csv_key,
            bucket_name=BRONZE_BUCKET,
            replace=True,
        )

        hook.delete_objects(
            bucket=STAGING_BUCKET,
            keys=[key],
        )


    files = list_xlsx_files()
    convert_single_file.expand(key=files)
