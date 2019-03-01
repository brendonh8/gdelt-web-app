import datetime as dt
import airflow
from airflow import DAG
from airflow.operators import BashOperator


default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=3)
}

dag = DAG('upload_raw_to_s3',
          default_args=default_args,
          schedule_interval='*/15 * * * *',
          )

s3_upload_confirm = BashOperator(task_id='s3_upload_confirm',
                                 bash_command="echo 'New files uploaded to S3 raw buckets'",
                                 dag=dag)

upload_raw_files = BashOperator(task_id='upload_raw_files',
                                bash_command='python /root/airflow/dags/extract/raw_file_collection.py',
                                dag=dag)

upload_raw_files >> s3_upload_confirm
