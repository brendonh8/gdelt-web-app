import airflowlib.emr_lib as emr
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2018, 2, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG to run every 3 hours
dag = DAG('geocam_sum', schedule_interval='* */3 * * *', default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)


# create an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='gdelt_cluster', num_core_nodes=2)
    return cluster_id


# wait for the EMR cluster to set up
def wait_for_completion(**kwargs):
    ti = kwargs['ti']  # task instance
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)


# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']  # task instance
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)


# Extract and sum geocam sentimental data
def transform_geocam_sum(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'spark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                                '/root/airflow/dags/transform/extract_timeseries.scala')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


# define tasks using python operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

transform_geocam = PythonOperator(
    task_id='transform_geocam',
    python_callable=transform_geocam_sum,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# construct the DAG by setting the dependencies
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> transform_geocam >> terminate_cluster
