import datetime as dt
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from functions.statsapi import *
from functions import check_game_today, call_games, call_standings, scrape_prob, write_insert_query, postgres_to_s3, delete_xcoms


#Instantiate DAG
with DAG(dag_id="voter_transformation",
         start_date = dt.datetime(2023,11,28), #Start Nov 28, 2023
         end_date = dt.datetime(2024,11,5), #End Election Day (
         schedule_interval="0 9 * * *", #run everyday at 9am Eastern 
         catchup=False,
        ) as dag:

    pull_raw_data = BashOperator(
        task_id='pull_raw_data_task',
        bash_callable = pull_raw.sh
        dag=dag
    )

    qc_raw = PythonOperator(
        task_id="qc_raw_task",
        python_callable= quality_control.qc_raw,
        dag=dag
    )

    qc_raw_fail = PythonOperator(
        task_id="qc_raw_fail_task",
        python_callable=quality_control.qc_raw_fail,
        dag=dag
    )

    transform_data = PythonOperator(
        task_id="transform_task",
        python_callable=quality_control.qc_raw_fail,
        dag=dag
    )

    qc_transformed = PythonOperator(
        task_id="qc_transformed",
        python_callable= quality_control.qc_transformed,
        dag=dag
    )

    qc_transformed_fail = PythonOperator(
        task_id="qc_transformed_fail_task",
        python_callable=quality_control.qc_transformed_fail,
        dag=dag
    )
          
    qc_transformed_succeed = PythonOperator(
        task_id="qc_transformed_succeed_task",
        python_callable=quality_control.qc_transformed_suceed,
        dag=dag
    )

    scrape_win_prob = PythonOperator(
        task_id = 'scrape_prob_task',
        python_callable = scrape_prob._scrape_prob,
        op_kwargs = {'short_team_name':'NYY',
                     'long_team_name':'Yankees'}, 
        dag=dag
    )
    
    write_insert_query= PythonOperator(
        task_id = 'write_insert_query_task',
        python_callable= write_insert_query._write_insert_query,
        dag=dag
    )

    create_sql_table = PostgresOperator(
        task_id='create_sql_table_task',
        postgres_conn_id='postgres_localhost',
        sql = '/include/create_table.sql'
    )

    exec_insert_query = PostgresOperator(
        task_id='exec_insert_query_task',
        postgres_conn_id='postgres_localhost',
        sql ='{{ ti.xcom_pull(key="insert_statements") }}'
    )

    postgres_to_s3 = PythonOperator(
        dag=dag,
        task_id="postgres_to_s3_task",
        python_callable=postgres_to_s3._postgres_to_s3
    )

    delete_xcoms = PythonOperator(
        task_id="delete_xcoms", 
        python_callable=delete_xcoms._delete_xcoms
    )

    end_dag = DummyOperator(
        task_id="end_dag",
        dag=dag
    )

#Set up dag dependencies
start_dag >> call_games >> check_game_today >> [game_today, end_dag]
game_today >> [call_standings, create_sql_table, scrape_win_prob] >> write_insert_query >> exec_insert_query >> [postgres_to_s3, delete_xcoms] >>  end_dag
