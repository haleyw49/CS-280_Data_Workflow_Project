from airflow import DAG
import logging as log
import requests
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import TaskInstance
from airflow.models import Variable

from models.config import Session #You would import this from your config file
from models.user import User
from models.tweet import Tweet

def first_task_function(ti: TaskInstance, **kwargs):
    log.info("load_data_task")
    
    session = Session()
    all_users = session.query(User).all() 
    
    user_id_list = []
    for user in all_users:
        user_id_list.append(user.user_id)
    ti.xcom_push("user ids", user_id_list)

    tweet_id_list = []
    all_tweets = session.query(Tweet).all()
    for tweet in all_tweets:
        tweet_id_list.append(tweet.tweet_id)
    ti.xcom_push("tweet ids", tweet_id_list)
    
    session.close()
    return

def get_auth_header():
    my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    return {"Authorization": f"Bearer {my_bearer_token}"}

def second_task_function(ti: TaskInstance, **kwargs):
    log.info("This is your second task")
    user_id_list = ti.xcom_pull(key="user ids", task_ids="load_data_task")
    tweet_id_list = ti.xcom_pull(key="tweet ids", task_ids="load_data_task")

    for id in user_id_list:
        tweets_url = f'https://api.twitter.com/1.1/statuses/user_timeline.json'
        response = requests.get(tweets_url, headers=get_auth_header(), params={"user_id": id, "count": 5})
        tweet_info = response.json()
        for tweet in tweet_info:
            tweet_id_list.append(tweet['id_str'])
            # go back to pass user who wrote tweet
    log.info(tweet_id_list)

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