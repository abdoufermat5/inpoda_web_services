import json
from datetime import timedelta, datetime

import pendulum
import requests
import tweepy
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.edgemodifier import Label
from zeep import Client, Settings
from zeep.exceptions import Fault
from suds.client import Client as ClientSuds
from zeep.helpers import serialize_object


# connect to the Twitter API
def getTwitterClient():
    API_KEY = "djOYUtUSHZQv8a1HurSe4crOw"
    API_SECRET = "q8zVIDk0WaUBN8SKMIxSYW4H3hPGaHrJG4F5jBpFrYemeGHQ0V"
    ACCESS_KEY = "1220826728790142976-vN8azoyd529EO1U3bRtAq3INkaftmU"
    ACCESS_SECRET = "y7AuvRFexNTAy3Dm9S9cEWX1UIKFAe7Ytj6R8lSbcqWH6"

    auth = tweepy.OAuthHandler(API_KEY, API_SECRET, ACCESS_KEY, ACCESS_SECRET)
    api = tweepy.API(auth)

    # auth = tweepy.OAuth2BearerHandler("AAAAAAAAAAAAAAAAAAAAANN5lgEAAAAAc7WrpYgbO0tzjA0eTsBR5Py7jxo
    # %3Dd46qbXgWecQPjnonYeYmGpQEyorIPkgmSsUQgpgNiCylCzBwla")
    # api = tweepy.API(auth)
    return api


def getSudsClient(WSDL_URL):
    return ClientSuds(WSDL_URL, faults=False, cachingpolicy=1, location=WSDL_URL)


def getZeepClient(url):
    settings = Settings(strict=False, xml_huge_tree=True)
    return Client(url, settings=settings)


BASE_URL = "https://f678-46-193-64-45.eu.ngrok.io"

client = getZeepClient(BASE_URL + "/ws/tweet?wsdl")


def makeUrlRequest(service_name, type, data):
    if type == "POST":
        URL = BASE_URL + "/" + service_name
        return requests.post(URL, data=data)
    elif type == "GET":
        URL = BASE_URL + "/" + service_name
        return requests.get(URL)


PORT_SERVER_1 = 8000
# DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # start date to now
    'start_date': pendulum.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'inpoda_twitter_data_preprocessing_and_analysis',
    tags=['twitter', 'data', 'ingestion', 'inpoda'],
    default_args=default_args,
    description='Inpoda Twitter data ingestion using Tweepy for fetching tweets and Zeep for SOAP calls',
    schedule_interval='0 * * * *',  # Schedule the DAG to run every hour
)


def get_tweet_from_twitter(**kwargs):
    # Get 1000 tweets related to the subjects (Transport, Immigration, Politique, Guerre, VIolence, LOi)
    # using the tweepy API

    api = getTwitterClient()
    tweets = api.search_tweets(q="Transport OR Immigration OR Politique OR Guerre OR Violence OR Loi",
                               include_entities=True,
                               count=100,
                               lang="fr",
                               tweet_mode='extended',
                               result_type="mixed", )
    all_tweets = [tweet._json for tweet in tweets]
    # convert the tweets to json using json library dumps method
    all_tweets_json = json.dumps(all_tweets, indent=4)
    # Pass the tweets to the next task as json objects
    kwargs['ti'].xcom_push(key='tweets', value=all_tweets_json)

    return all_tweets


def db_is_full(ds, **kwargs):
    all_tweeets = client.service.getAllTweets()
    if len(all_tweeets) > 0:
        all_tweeets = [dict(tweet) for tweet in serialize_object(all_tweeets)]
        all_tweets_json = json.dumps(all_tweeets, indent=4)
        # Pass the tweets to the next task as json objects
        kwargs['ti'].xcom_push(key='tweets', value=all_tweets_json)
        return all_tweeets
    else:
        return []


def decide_tweet_source(ds, **kwargs):
    # Get the tweets from the database
    db_all_tweeets = client.service.getAllTweets()
    if db_all_tweeets is not None and len(db_all_tweeets) > 0:
        db_all_tweeets = [dict(tweet) for tweet in serialize_object(db_all_tweeets)]

        if len(db_all_tweeets) <= 100:
            # push to the next task
            kwargs['ti'].xcom_push(key='decide_tweet_source', value="get_tweet_from_twitter")
            return 'get_tweet_from_twitter'
        else:
            kwargs['ti'].xcom_push(key='decide_tweet_source', value="db_is_full")
            return 'db_is_full'
    else:
        kwargs['ti'].xcom_push(key='decide_tweet_source', value="get_tweet_from_twitter")
        return 'get_tweet_from_twitter'


def publishTweets(ds, **kwargs):
    tweets = kwargs['ti'].xcom_pull(key='tweets', task_ids='get_tweet_from_twitter')
    tweets = json.loads(tweets)
    for tweet in tweets:
        topics = []
        if "entities" in tweet:
            for annotation in tweet["entities"]:
                if "domain" in annotation:
                    if "name" in annotation["domain"]:
                        topics.append(annotation["domain"]["name"])
        if len(topics) == 0:
            topic = "General"
        else:
            topic = "<->".join(topics)
        tweet["topic"] = topic
        if "full_text" in tweet:
            tweet["text"] = tweet["full_text"]
        tweet["author_id"] = tweet["user"]["id"]
        try:
            response = client.service.putTweet(tweet)
            kwargs['ti'].xcom_push(key='publish_result', value=response)
        except Fault as e:
            response = "Error"
            raise e
        print(response)


def identifyAuthor(ds, **kwargs):
    authors = []
    task_ids = kwargs['ti'].xcom_pull(key='decide_tweet_source', task_ids='decide_tweet_source')
    for tweet in json.loads(kwargs['ti'].xcom_pull(key='tweets', task_ids=task_ids)):
        response = client.service.identifyAuthor(tweet["id"])
        print(response)
        authors.append(response)
    kwargs['ti'].xcom_push(key='authors', value=authors)


def extractHashtags(ds, **kwargs):
    hashtags = []
    task_ids = kwargs['ti'].xcom_pull(key='decide_tweet_source', task_ids='decide_tweet_source')
    for tweet in json.loads(kwargs['ti'].xcom_pull(key='tweets', task_ids=task_ids)):
        response = client.service.extractTweetHashtag(tweet["id"])
        print(response)
        hashtags.append(response)

    # Store the results in XCOM for future use
    kwargs['ti'].xcom_push(key='hashtags', value=hashtags)


def identifyTopics(ds, **kwargs):
    topics = []
    task_ids = kwargs['ti'].xcom_pull(key='decide_tweet_source', task_ids='decide_tweet_source')
    for tweet in json.loads(kwargs['ti'].xcom_pull(key='tweets', task_ids=task_ids)):
        response = client.service.identifyTopic(tweet["id"])
        print(response)
        topics.append(response)

    # Store the results in XCOM for future use
    kwargs['ti'].xcom_push(key='topics', value=topics)


def sentimentAnalysis(**kwargs):
    POSITIVE = []
    NEGATIVE = []
    NEUTRAL = []
    task_ids = kwargs['ti'].xcom_pull(key='decide_tweet_source', task_ids='decide_tweet_source')
    for tweet in json.loads(kwargs['ti'].xcom_pull(key='tweets', task_ids=task_ids)):
        response = client.service.predictSentiment(tweet["id"])
        print(response)

        # Store the results in XCOM for future use
        # if sentiment is NEGATIVE or POSITIVE or NEUTRAL store it in XCOM with
        # the tweet
        if response in ["NEGATIVE", "POSITIVE", "NEUTRAL"]:
            if response == "NEGATIVE":
                NEGATIVE.append(tweet)
            elif response == "POSITIVE":
                POSITIVE.append(tweet)
            elif response == "NEUTRAL":
                NEUTRAL.append(tweet)
    kwargs['ti'].xcom_push(key='POSITIVE', value=POSITIVE)
    kwargs['ti'].xcom_push(key='NEGATIVE', value=NEGATIVE)
    kwargs['ti'].xcom_push(key='NEUTRAL', value=NEUTRAL)


def getTopKHashtag(ds, **kwargs):
    try:
        result = client.service.getTopKHashTag(10)

        if len(result) != 0:
            ti = kwargs['ti']
            ti.xcom_push(key='top_hashtag', value=result)
            return True
        else:
            raise ValueError("The number of returned tweets is not equal to the specified value of k.")
    except Fault as ex:
        raise ValueError("The request to the SOAP service failed. Error: {}".format(ex))


def getTopKUser(ds, **kwargs):
    try:
        result = client.service.getTopKUser(10)

        if len(result) != 0:
            ti = kwargs['ti']
            ti.xcom_push(key='top_publications', value=result)
            return True
        else:
            raise ValueError("The number of returned tweets is not equal to the specified value of k.")
    except Fault as ex:
        raise ValueError("The request to the SOAP service failed. Error: {}".format(ex))


def getTopKtopics(ds, **kwargs):
    try:
        result = client.service.getTopKTopic(10)

        if len(result) != 0:
            ti = kwargs['ti']
            ti.xcom_push(key='top_topics', value=result)
            return True
        else:
            raise ValueError("The number of returned tweets is not equal to the specified value of k.")
    except Fault as ex:
        raise ValueError("The request to the SOAP service failed. Error: {}".format(ex))


def getPositiveSentiment(ds, **kwargs):
    try:
        result = client.service.getPositiveSentiment(10)

        if len(result) != 0:
            ti = kwargs['ti']
            ti.xcom_push(key='top_positive', value=result)
            return True
        else:
            raise ValueError("The number of returned tweets is not equal to the specified value of k.")
    except Fault as ex:
        raise ValueError("The request to the SOAP service failed. Error: {}".format(ex))


def getNegativeSentiment(ds, **kwargs):
    try:
        result = client.service.getNegativeSentiment(10)

        if len(result) != 0:
            ti = kwargs['ti']
            ti.xcom_push(key='top_negative', value=result)
            return True
        else:
            raise ValueError("The number of returned tweets is not equal to the specified value of k.")
    except Fault as ex:
        raise ValueError("The request to the SOAP service failed. Error: {}".format(ex))


def getNeutralSentiment(ds, **kwargs):
    try:
        result = client.service.getNeutralSentiment(10)

        if len(result) != 0:
            ti = kwargs['ti']
            ti.xcom_push(key='top_neutral', value=result)
            return True
        else:
            raise ValueError("Le nombre de tweets retournés n'est pas égal à la valeur spécifiée de k.")
    except Fault as ex:
        raise ValueError("The request to the SOAP service failed. Error: {}".format(ex))


def retrieveTopKtopics(ds, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(key='top_topics', task_ids='getTopKtopics')
    # Do something with the result, for example log it or store it in
    # a database.
    print(result)


def retrieveTopKUser(ds, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(key='top_publications', task_ids='getTopKUser')
    # Do something with the result, for example log it or store it in
    # a database.
    print(result)


def retrieveTopKHashtag(ds, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(key='top_hashtag', task_ids='getTopKHashtag')
    # Do something with the result, for example log it or store it in
    # a database.
    print(result)


def retrievePositiveSentiment(ds, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(key='top_positive', task_ids='getPositiveSentiment')
    # Do something with the result, for example log it or store it in
    # a database.
    print(result)


def retrieveNegativeSentiment(ds, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(key='top_negative', task_ids='getNegativeSentiment')
    # Do something with the result, for example log it or store it in
    # a database.
    print(result)


def retrieveNeutralSentiment(ds, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(key='top_neutral', task_ids='getNeutralSentiment')
    # store to airflow postgresql database
    print(result)


start = DummyOperator(task_id='start', dag=dag)

decide_tweet_source_task = PythonOperator(
    task_id='decide_tweet_source',
    python_callable=decide_tweet_source,
    provide_context=True,
    dag=dag
)

db_is_full_task = PythonOperator(
    task_id='db_is_full',
    trigger_rule='one_success',
    python_callable=db_is_full,
    dag=dag
)

# Define the tasks in the DAG
get_tweet_from_twitter_task = PythonOperator(
    task_id='get_tweet_from_twitter',
    trigger_rule='one_success',
    python_callable=get_tweet_from_twitter,
    dag=dag
)

put_tweet_task = PythonOperator(
    task_id='put_tweet',
    python_callable=publishTweets,
    provide_context=True,
    dag=dag
)

identify_author_task = PythonOperator(
    task_id='identify_author',
    python_callable=identifyAuthor,
    provide_context=True,
    dag=dag
)

extract_hashtags_task = PythonOperator(
    task_id='extract_hashtags',
    python_callable=extractHashtags,
    provide_context=True,
    dag=dag
)

identify_topics_task = PythonOperator(
    task_id='identify_topics',
    python_callable=identifyTopics,
    provide_context=True,
    dag=dag
)

analyse_sentiment_task = PythonOperator(
    task_id='analyse_sentiment',
    python_callable=sentimentAnalysis,
    provide_context=True,
    dag=dag
)

"""get_top_k_hashtag_task = PythonOperator(
    task_id='get_top_k_hashtag',
    python_callable=getTopKHashtag,
    provide_context=True,
    op_args=[],
    dag=dag,
)

get_top_k_publications_task = PythonOperator(
    task_id='get_top_k_publications',
    python_callable=getTopKUser,
    provide_context=True,
    op_args=[],
    dag=dag,
)

get_top_k_topics_task = PythonOperator(
    task_id='get_top_k_topics',
    python_callable=getTopKtopics,
    provide_context=True,
    op_args=[],
    dag=dag,
)

get_positive_sentiment_task = PythonOperator(
    task_id='get_positive_sentiment',
    python_callable=getPositiveSentiment,
    provide_context=True,
    op_args=[],
    dag=dag,
)

get_negative_sentiment_task = PythonOperator(
    task_id='get_negative_sentiment',
    python_callable=getNegativeSentiment,
    provide_context=True,
    op_args=[],
    dag=dag,
)

get_neutral_sentiment_task = PythonOperator(
    task_id='get_neutral_sentiment',
    python_callable=getNeutralSentiment,
    provide_context=True,
    op_args=[],
    dag=dag,
)

retrieve_result_top_k_hashtag_task = PythonOperator(
    task_id='retrieve_result_top_k_hashtag_task',
    python_callable=retrieveTopKHashtag,
    provide_context=True,
    dag=dag,
)

retrieve_result_top_k_user_task = PythonOperator(
    task_id='retrieve_result_top_k_user_task',
    python_callable=retrieveTopKUser,
    provide_context=True,
    dag=dag,
)

retrieve_result_top_k_topics_task = PythonOperator(
    task_id='retrieve_result_top_k_topics_task',
    python_callable=retrieveTopKtopics,
    provide_context=True,
    dag=dag,
)

retrieve_result_positive_sentiment_task = PythonOperator(
    task_id='retrieve_result_positive_sentiment_task',
    python_callable=retrievePositiveSentiment,
    provide_context=True,
    dag=dag,
)

retrieve_result_negative_sentiment_task = PythonOperator(
    task_id='retrieve_result_negative_sentiment_task',
    python_callable=retrieveNegativeSentiment,
    provide_context=True,
    dag=dag,
)

retrieve_result_neutral_sentiment_task = PythonOperator(
    task_id='retrieve_result_neutral_sentiment_task',
    python_callable=retrieveNeutralSentiment,
    provide_context=True,
    dag=dag,
)"""

end_task = DummyOperator(
    task_id='end',
    dag=dag
)

start >> decide_tweet_source_task

# Define the dependencies between the tasks
decide_tweet_source_task >> get_tweet_from_twitter_task >> put_tweet_task >> [identify_author_task,
                                                                              extract_hashtags_task,
                                                                              identify_topics_task,
                                                                              analyse_sentiment_task] >> end_task
decide_tweet_source_task >> db_is_full_task >> Label('DATABASE IS FULL') >> end_task
