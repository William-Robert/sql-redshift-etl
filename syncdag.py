from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from mssqlmachine import mssqlMachine
from redshiftmachine import redshiftmachine
from s3machine import s3machine

from con_defs import sql_params as sp
from con_defs import redshift_params as rs
import boto3

def sync_startup():
    sql = mssqlMachine(sp)
    redshift = redshiftmachine(rs)
    s = s3machine(sql, redshift)
    s.startup_procedures()
    return 'ran startup function'

def sync_src_runner():
    sql = mssqlMachine(sp)
    redshift = redshiftmachine(rs)
    s = s3machine(sql, redshift)
    s.src_runner()
    return 'ran src fuction'

def sync_target_runner():
    sql = mssqlMachine(sp)
    redshift = redshiftmachine(rs)
    s = s3machine(sql, redshift)
    s.target_runner()
    return 'ran target function'

dag = DAG('SYNC-MACHINE', description='This the db sync machine', schedule_interval=None, start_date=datetime(2017, 3, 20), catchup=False)



startup_operator = PythonOperator(task_id='start_task', python_callable=sync_startup, dag=dag)
src_operator = PythonOperator(task_id='src_task', python_callable=sync_src_runner, dag=dag)
target_operator = PythonOperator(task_id='target_task', python_callable=sync_target_runner, dag=dag)

startup_operator >> src_operator >> target_operator







