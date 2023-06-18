from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
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

# Variables
URL = data['url']
PARAMS = data['params_faliro']
USER_AGENT = UserAgent().chrome
TEST_DB = "test"
TEST_COLLECTION = 'most_recent'

def conn_mongo(database_name, collection_name) -> str:
    """
    The conn_mongo function establishes a connection to a MongoDB database. It tries to connect to the MongoDB server using the provided 
    credentials and checks if the specified database collection exists. If the collection exists, it returns the name of the collection. 
    If the collection does not exist, it raises an exception. If there is any error during the connection process, 
    it raises an exception with an error message.
    """
    try:
        client = MongoClient('mongo:27017', username='root', password='password') # authMechanism='SCRAM-SHA-256'
        """
        authMechanism='SCRAM-SHA-256' specifies the authentication mechanism to be used when connecting to MongoDB. SCRAM-SHA-256 
        (Salted Challenge Response Authentication Mechanism) is a secure authentication mechanism that protects the credentials during the authentication process.
        """
        db = client[database_name]
        collection_names = db.list_collection_names()
        client.close()

        # Check if the collection exists
        if collection_name in collection_names:
            print(f"Connected to MongoDB. Collection '{collection_name}' exists in database '{database_name}'.")
            return True
        else:
            print(f"Connected to MongoDB. Collection '{collection_name}' does not exist in database '{database_name}'.")
            return False
    except Exception as e:
        raise Exception(f"Failed to connect to MongoDB: {str(e)}")

def transfer_to_database(database_name, collection_name, **context) -> bool:
    """
    It handles the data transfer process to a MongoDB database. Here's a summary of what the function does:
    It retrieves the data from the previous task execution by using the xcom_pull method with the task ID 'scrape_data'.
    Then it establishes a connection to the MongoDB database and inserts the data into the collection, passing the values from the data dictionary.
    It saves the length of the data in the task's context and finally
    returns True to indicate the successful completion of the data transfer process.
    """
    data = context['ti'].xcom_pull(task_ids='scrape_data', key='return_value')
    if data:
        client = MongoClient('mongo:27017', username='root', password='password')

        # Establish MongoDB connection
        db = client[database_name]
        collection = db[collection_name]
        
        # Insert documents into the collection
        collection.insert_many(data.values())

        len_data = len(data)
        print(f"The number of items was: {len_data}")
        # Save the length of data in the task's context
        context['ti'].xcom_push(key='data_length', value=len_data)

        client.close()

        return True


with DAG(
         'web_scraper_testing', 
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
        op_kwargs={'database_name': TEST_DB, 'collection_name': TEST_COLLECTION},
        dag=dag
    )

    scrape_data_ = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_all_data,
        op_kwargs={'url': URL, 'params': PARAMS, 'user_agent': USER_AGENT},
        dag=dag
    )

    transfer_to_db = PythonOperator(
        task_id='transfer_to_database',
        python_callable=transfer_to_database,
        op_kwargs={'database_name': TEST_DB, 'collection_name': TEST_COLLECTION},
        dag=dag
    )


    mongo_connection >> scrape_data_ >> transfer_to_db 

