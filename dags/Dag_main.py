from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import os
import json

"""
The DAG consists of the following tasks:

    1, scrape_hrefs_task: A PythonOperator task that calls the scrape_hrefs function, passing the URL and params as op_kwargs. This task is responsible for scraping hrefs.

    2. add_hrefs_to_mongo_task: A PythonOperator task that calls the add_hrefs_to_mongo function, specifying the database name and collection name as op_kwargs. 
       This task adds the scraped hrefs to a MongoDB collection.

    3. calculate_num_dags_task: A PythonOperator task that calls the calculate_num_dags function and provides the context. This task calculates the number 
       of DAGs needed and the delay between each DAG run based on the number of hrefs.

    4. create_dags_task: A PythonOperator task that calls the create_dags function and provides the context. 
       This task generates multiple DAGs based on the calculated number of DAGs and the delay between each DAG run.
"""

file_path = os.path.join(os.path.dirname(__file__), "app")
# Import the scraper module
from app.main import *
from app.for_the_dags import *

# Open and load the JSON file with the necessary parameters
with open("app/params.json", "r") as file:
    data = json.load(file)

DB = "test"
COLLECTION_HREFS = "hrefs"
URL = data["url"]
PARAMS = data["params"]
COLLECTION_ADVS = "rentals"
MAX_HREFS_PER_DAG = 50
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 6, 8),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}


def create_dags(**context):
    """
    It generates multiple DAGs based on the calculated number of DAGs and the delay between each DAG run. 
    It retrieves the number of DAGs and the delay from the task context using XCom. The function then prints 
    the number of DAGs and the delay for informational purposes. It initializes the current time and creates 
    a schedule list to store DAG configuration details. It iterates over the range of the number of DAGs 
    and creates a unique DAG name, start time, and cron expression for each DAG. The DAG configurations 
    are appended to the schedule list. 
    Finally, the schedule list is dumped into a JSON file named 'schedule.json' for future reference.
    """
    num_dags = context['ti'].xcom_pull(task_ids='calculate_num_dags', key='num_dags')
    delay = context['ti'].xcom_pull(task_ids='calculate_num_dags', key='delay_hours')

    print(f'Number of dags: {num_dags}\nHours of delay: {delay}')

    current_time = datetime.datetime.now()
        
    schedule = []

    for i in range(1, num_dags + 1):
        dag_name = f'dag_{i}'
        start_time = current_time + timedelta(hours=delay * i)
        cron_expression = f"{start_time.minute} {start_time.hour} * * *"
        dag_config = {'dag_name': dag_name, 'cron': cron_expression}
        print(f'Dag config: {dag_config}')
        schedule.append(dag_config)

    schedule_file_path = os.path.join('/opt/airflow/app', 'schedule.json')
    with open(schedule_file_path, 'w') as schedule_file:
        json.dump(schedule, schedule_file)


with DAG(
        'web_scraper',
        default_args=default_args,
        schedule_interval=None,
        description="DAG is used to store hrefs."
        ) as dag_scraper:

        scrape_hrefs_task = PythonOperator(
            task_id='scrape_hrefs',
            python_callable=scrape_hrefs,
            op_kwargs={'url': URL, 'params': PARAMS},
            dag=dag_scraper
        )

        add_hrefs_to_mongo_task = PythonOperator(
            task_id='add_hrefs_to_mongo',
            python_callable=add_hrefs_to_mongo,
            op_kwargs={'database_name': DB, 'collection_name': COLLECTION_HREFS},
            dag=dag_scraper
        )

        send_slack_number_of_hrefs_task = SlackWebhookOperator(
            task_id="send_slack_number_of_hrefs",
            http_conn_id="slack_conn",
            message= send_slack_number_of_hrefs(),
            channel="#monitoring",
            dag=dag_scraper
        )

        calculate_num_dags_task = PythonOperator(
            task_id='calculate_num_dags',
            python_callable=calculate_num_dags,
            provide_context=True,
            dag=dag_scraper
        )

        create_dags_task = PythonOperator(
            task_id='create_dags',
            python_callable=create_dags,
            provide_context=True,
            dag=dag_scraper
        )

        scrape_hrefs_task >> add_hrefs_to_mongo_task >> calculate_num_dags_task >> create_dags_task
        add_hrefs_to_mongo_task >> send_slack_number_of_hrefs_task

