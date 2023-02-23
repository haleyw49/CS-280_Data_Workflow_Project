from airflow import DAG
import logging as log
import requests
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import TaskInstance
from airflow.models import Variable
import pandas as pd 
from models.config import Session #You would import this from your config file
from models.user import User
from models.tweet import Tweet
from models.user_timeseries import User_Timeseries
from models.tweet_timeseries import Tweet_Timeseries
from google.cloud import storage
from gcsfs import GCSFileSystem
import Datetime

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

    # get user stats for all users in database
    user_responses = []
    for user_id in user_id_list:
        api_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics,profile_image_url,username,description,id,created_at"
        request = requests.get(api_url, headers=get_auth_header())
        user_responses.append(request.json())
    ti.xcom_push("user responses", user_responses)

    # get 5 most recent tweets for all users in database
    user_tweet_responses = []
    for id in user_id_list:
        tweets_url = f'https://api.twitter.com/1.1/statuses/user_timeline.json'
        response = requests.get(tweets_url, headers=get_auth_header(), params={"user_id": id, "count": 5})
        tweet_info = response.json()
        for tweet in tweet_info:
            tweet_id_list.append(tweet['id_str'])
            # go back to pass user who wrote tweet
    log.info(tweet_id_list)


    # get tweet stats for all tweets
    tweet_responses = []
    for tweet_id in tweet_id_list:
        api_url = f"https://api.twitter.com/2/tweets/{tweet_id}?tweet.fields=public_metrics,author_id,text,created_at"
        request = requests.get(api_url, headers=get_auth_header())
        tweet_responses.append(request.json())
    ti.xcom_push("tweet responses", tweet_responses)

    return

def third_task_function(ti: TaskInstance, **kwargs):
    log.info("This is your third task")
    user_responses = ti.xcom_pull(key="user responses", task_ids="call_api_task")
    tweet_responses = ti.xcom_pull(key="tweet responses", task_ids="call_api_task")
    
    user_df = pd.DataFrame(columns=['user_id', 'username', 'name', 'created_at', 'followers_count', 'following_count', 'tweet_count', 'listed_count'])
    for item in user_responses:
        user_id = item['data']['id']
        username = item['data']['username']
        name = item['data']['name']
        created_at = item['data']['created_at']
        followers = item['data']['public_metrics']['followers_count']
        following = item['data']['public_metrics']['following_count']
        tweet_count = item['data']['public_metrics']['tweet_count']
        listed_count = item['data']['public_metrics']['listed_count']

        user_df.loc[len(user_df.index)] = [user_id, username, name, created_at, followers, following, tweet_count, listed_count] 


    tweet_df = pd.DataFrame(columns=['tweet_id', 'user_id', 'text', 'retweet_count', 'like_count', 'created_at'])
    for item in tweet_responses:
        tweet_id = item['data']['id']
        text = item['data']['text']
        retweets = item['data']['public_metrics']['retweet_count']
        likes = item['data']['public_metrics']['like_count']
        tweet_user_id=item['data']['author_id']
        tweet_created_at=item['data']['author_id']

        tweet_df.loc[len(tweet_df.index)] = [tweet_id, tweet_user_id, text, retweets, likes, tweet_created_at]
    
    client = storage.Client()
    bucket = client.get_bucket("h-w-apache-airflow-cs280")
    bucket.blob("data/proj2_user_data.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")
    bucket.blob("data/proj2_tweet_data.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")
    
    return



def fourth_task_function():
    log.info("This is your third task")

    fs = GCSFileSystem(project="haley-wiese-cs-280")
    with fs.open('h-w-apache-airflow-cs280/data/proj2_user_data.csv', "r") as file_obj:
        user_df = pd.read_csv(file_obj)
    with fs.open('h-w-apache-airflow-cs280/data/proj2_tweet_data.csv', "r") as file_obj:
        tweet_df = pd.read_csv(file_obj)

    user_set = set(user_df['user_id'])
    tweet_set = set(tweet_df['tweet_id'])

    session = Session()
    for user in user_set:
        user = user_df[user_df['user_id'] == user].iloc[-1]
        user = user.to_dict()
        
        add_user_timeseries = User_Timeseries(
            user_id=user['user_id'],
            followers_count=user['followers_count'],
            following_count=user['following_count'],
            tweet_count=user['tweet_count'],
            listed_count=user['listed_count'],
            date=Datetime.now()
        )
        session.add(add_user_timeseries)
        session.commit()

    for tweet in tweet_set:
        tweet = tweet_df[tweet_df['tweet_id'] == tweet].iloc[-1]
        tweet = tweet.to_dict()

        add_tweet = Tweet(
            tweet_id=tweet['tweet_id'],
            user_id=tweet['user_id'],
            text=tweet['text'],
            created_at=tweet['created_at']
        )
        session.add(add_tweet)

        add_tweet_timeseries = Tweet_Timeseries(
            tweet_id=tweet['tweet_id'],
            retweet_count=tweet['retweet_count'],
            favorite_count=tweet['like_count'],
            date=Datetime.now()
        )
        session.add(add_tweet_timeseries)
        session.commit()

    session.close()
    log.info('End of process')
    return

with DAG(
    dag_id="project_two_dag",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023,2, 26, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    first_task = PythonOperator(task_id="load_data_task", python_callable=first_task_function)
    second_task = PythonOperator(task_id="call_api_task", python_callable=second_task_function)
    third_task = PythonOperator(task_id="transform_data_task", python_callable=third_task_function)
    fourth_task = PythonOperator(task_id="write_data_task", python_callable=fourth_task_function)
    end_task = DummyOperator(task_id="end_task")

start_task >> first_task >> second_task >> third_task >> fourth_task >> end_task