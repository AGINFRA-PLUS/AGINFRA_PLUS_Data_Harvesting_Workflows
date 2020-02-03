"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'AGINFRA',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 28),
    'email': [''],
    'email_on_failure': True
}

dag = DAG('AginfraTransformSentToDB', default_args=default_args, schedule_interval=timedelta(days=1))


e2_Aginfra_storage = BashOperator(dag=dag,
                              task_id='AginfraStorage',
                              bash_command='python3 path/AginfraTransfSendToDB.py ')

e4_Zenodo_storage = BashOperator(dag=dag,
                              task_id='ZenodoStorage',
                              bash_command='python3 path/ZenodoTransfSendToDB.py ')

e6_Openaire_storage = BashOperator(dag=dag,
                              task_id='OpenaireStorage',
                              bash_command='python3 path/OpenaireTransfSendToDB.py ')

e8_Gardian_storage = BashOperator(dag=dag,
                              task_id='GardianStorage',
                              bash_command='python3 path/GardianTransfSendToDB.py ')







