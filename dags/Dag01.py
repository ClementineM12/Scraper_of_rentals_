from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from pymongo import MongoClient
import json 
import os

file_path = os.path.join(os.path.dirname(__file__), "app")
# Import the scraper module
from app.main import *

# Open and load the JSON file with the necessary parameters
with open("app/params.json", "r") as file:
    data = json.load(file)

url = data['url']
params = data["params_images"]

TEST_DB_NAME = "test"
TEST_COLLECTION_DB_NAME = 'users'

def conn_mongo(test_db = TEST_DB_NAME) -> str:
    """
    The conn_mongo function establishes a connection to a MongoDB database. It tries to connect to the MongoDB server using the provided 
    credentials and checks if the specified database collection exists. If the collection exists, it returns the name of the collection. 
    If the collection does not exist, it raises an exception. If there is any error during the connection process, 
    it raises an exception with an error message.
    """
    try:
        client = MongoClient('mongo:27017', username='root', password='password')
        """
            authMechanism='SCRAM-SHA-256' specifies the authentication mechanism to be used when connecting to MongoDB. SCRAM-SHA-256 
            (Salted Challenge Response Authentication Mechanism) is a secure authentication mechanism that protects the credentials during the authentication process.
        """
        db_names = client.list_database_names()
        client.close()
        # Check if the collection exists
        if test_db in db_names:
            print(f"Connected to MongoDB. Collection '{test_db}' exists.")
            return test_db
        else:
            raise Exception(f"Connected to MongoDB, but collection '{test_db}' does not exist.")
    except Exception as e:
        raise Exception(f"Failed to connect to MongoDB: {str(e)}")

def transfer_to_database(**context) -> bool:
    """
    It handles the data transfer process to a MongoDB database. Here's a summary of what the function does:
    It retrieves the data from the previous task execution by using the xcom_pull method with the task ID 'scrape_data'.
    Then it establishes a connection to the MongoDB database and inserts the data into the collection, passing the values from the data dictionary.
    It saves the length of the data in the task's context and finally
    returns True to indicate the successful completion of the data transfer process.
    """
    data = context['task_instance'].xcom_pull(task_ids='scrape_data')
    client = MongoClient('mongo:27017', username='root', password='password')

    # Establish MongoDB connection
    db = client['test']
    collection = db['users']
    
    # Insert documents into the collection
    collection.insert_many(data.values())

    client.close()

    # Save the length of data in the task's context
    context['ti'].xcom_push(key='data_length', value=len(data))

    return True

def slack(**context) -> str:
    """
    It is responsible for generating a summary message based on the outcome of the data transfer process.
    It retrieves the context passed to the function, which contains information from the previous task execution and returns the generated message.
    """
    try:
        data_length = context['task_instance'].xcom_pull(task_ids='transfer_to_database')['data_length']
        message = f"Data transfer complete. Total records transferred: {data_length}"
    except KeyError:
        message = "No data were transferred.."
    
    return message


with DAG(
         'web_scraper_', 
         default_args={ 
                     'owner': 'airflow',
                     'depends_on_past': False,
                     'start_date': datetime.datetime(2023, 6, 8),
                     'retries': 3,
                     'retry_delay': timedelta(minutes=1),
                     }, 
         schedule_interval=None
        ) as dag:

    mongo_connection = PythonOperator(
        task_id='mongo_connection',
        python_callable=conn_mongo,
        dag=dag
    )

    scrape_data_ = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_all_data,
        op_args=[url],  # Pass arguments without `params`
        op_kwargs={'params': params},  # Pass `params` as a keyword argument
        provide_context=True,
        dag=dag
    )

    transfer_to_db = PythonOperator(
        task_id='transfer_to_database',
        python_callable=transfer_to_database,
        provide_context=True,
        dag=dag
    )

    send_slack_notification = SlackWebhookOperator(
        task_id="send_slack_notification",
        http_conn_id="slack_conn",
        message= slack(),
        channel="#monitoring",
        dag=dag
    )

    mongo_connection >> scrape_data_ >> transfer_to_db >> send_slack_notification

