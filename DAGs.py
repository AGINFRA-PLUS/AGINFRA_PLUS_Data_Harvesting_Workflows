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

dag = DAG('Aginfra Harvesting', default_args=default_args, schedule_interval=timedelta(days=1))

e1_Aginfra_harvesting = BashOperator(dag=dag,
                              task_id='Aginfra_harvesting',
                              bash_command='path/Harvest_from_Aginfra.py ')
e2_Aginfra_storage = BashOperator(dag=dag,
                              task_id='Aginfra_storage',
                              bash_command='path/AginfraTransfSendToDB.py ')
e3_Zenodo_harvesting = BashOperator(dag=dag,
                              task_id='Zenodo_harvesting',
                              bash_command='path/Harvest_from_Zenodo.py ')
e4_Zenodo_storage = BashOperator(dag=dag,
                              task_id='Zenodo_storage',
                              bash_command='path/ZenodoTransfSendToDB.py ')
e5_Openaire_harvesting = BashOperator(dag=dag,
                              task_id='Openaire_harvesting',
                              bash_command='path/Harvest_from_Openairepy ')
e6_Openaire_storage = BashOperator(dag=dag,
                              task_id='Openaire_storage',
                              bash_command='path/OpenaireTransfSendToDB.py ')
e7_Gardian_harvesting = BashOperator(dag=dag,
                              task_id='Gardian_harvesting',
                              bash_command='path/Harvest_from_Gardian.py ')
e8_Gardian_storage = BashOperator(dag=dag,
                              task_id='Gardian_storage',
                              bash_command='path/GardianTransfSendToDB.py ')







