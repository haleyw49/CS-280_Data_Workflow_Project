from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from models.config import Session #You would import this from your config file
from models.user import User

def first_task_function():
    log.info("load_data_task")
    session = Session()

    # This will retrieve all of the users from the database 
    # (It'll be a list, so you may have 100 users or 0 users)
    session.query(User).all() 

    # This will retrieve the user who's username is NASA
    espnUser = session.query(User).filter(User.username == "espn").first()

    #You can then print the username of the user you retrieved
    print(espnUser.username)

    #We recommend that you reassign the user to a variable so that you can use it later
    nasaUsername = espnUser.username

    # This will close the session that you opened at the beginning of the file.
    session.close()
    
    return

def second_task_function():
    log.info("This is your second task")
    return

def third_task_function():
    log.info("This is your third task")
    return

def fourth_task_function():
    log.info("This is your third task")
    return

with DAG(
    dag_id="project_two_dag",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    first_task = PythonOperator(task_id="load_data_task", python_callable=first_task_function)
    second_task = PythonOperator(task_id="call_api_task", python_callable=second_task_function)
    third_task = PythonOperator(task_id="transform_data_task", python_callable=third_task_function)
    fourth_task = PythonOperator(task_id="write_data_task", python_callable=fourth_task_function)
    end_task = DummyOperator(task_id="end_task")

start_task >> first_task >> second_task >> third_task >> fourth_task >> end_task