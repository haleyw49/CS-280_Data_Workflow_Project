from airflow import DAG
import logging as log
import pendulum
import random
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def get_random_numbers():
    log.info("First task: Get random numbers")
    x = random.randint(0,20)
    y = random.randint(20,50)
    return x, y

def first_task_function():
    log.info("Add numbers together")
    x, y = get_random_numbers()
    log.info(f"{x} + {y} = {x+y}")
    return

def second_task_function():
    log.info("I don't like math")
    return

with DAG(
    dag_id="my_practice_dag",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    first_task = PythonOperator(task_id="first_task", python_callable=first_task_function)
    second_task = PythonOperator(task_id="second_task", python_callable=second_task_function)
    end_task = DummyOperator(task_id="end_task")

start_task >> first_task >> second_task >> end_task
